from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("monthly_fee", "id")
w2 = Window.partitionBy("plan", "customer_id")
w3 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")
w4 = Window.partitionBy("plan", "customer_id").orderBy(F.col("next_monthly_fee").desc_nulls_last())
w5 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee_bucket").desc_nulls_first())
w6 = Window.partitionBy("status")
w7 = Window.partitionBy("plan")

scored = (
    df_subscriptions
    .select(
        "*",
        F.ntile(5).over(w).alias("monthly_fee_bucket"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
        F.lead("monthly_fee", 1).over(w3).alias("next_monthly_fee"),
    )
)

kept_keys = (
    scored
    .select("status")
    .intersect(
        df_subscriptions
        .filter(F.col("monthly_fee").isNull())
        .select("status")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

scored_subscriptions = (
    reconciled
    .withColumn("next_monthly_fee_percent_rank", F.percent_rank().over(w4))
    .withColumn("monthly_fee_bucket_percent_rank", F.percent_rank().over(w5))
    .withColumn("monthly_fee_share_2", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w6)), 4))
    .withColumn("monthly_fee_bucket_share", F.round((F.col("monthly_fee_bucket") / F.sum("monthly_fee_bucket").over(w7)), 4))
    .withColumn("next_monthly_fee_share", F.round((F.col("next_monthly_fee") / F.sum("next_monthly_fee").over(w7)), 4))
)

kept_keys_2 = (
    scored_subscriptions
    .select("customer_id")
    .exceptAll(
        df_subscriptions
        .filter(F.col("plan") != "standard")
        .select("customer_id")
    )
)

verified = (
    scored_subscriptions
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

matched_keys = (
    verified
    .select("customer_id")
    .subtract(
        df_subscriptions
        .filter(F.col("monthly_fee").between(170, 240))
        .select("customer_id")
    )
)

kept_rows = (
    verified
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.segment"))
)

result = (
    with_dims
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "status",
        "monthly_fee_bucket",
        "monthly_fee_share",
        "next_monthly_fee",
        F.when(F.col("status").rlike("^(EXPIRED|ACTIVE)$"), "OTHER_ZONE").otherwise("CORE").alias("status_zone"),
    )
    .orderBy(F.col("start_date").asc_nulls_last())
)
