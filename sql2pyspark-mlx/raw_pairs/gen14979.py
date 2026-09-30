from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("end_date", "id")
w2 = Window.orderBy("monthly_fee", "id")
w3 = Window.partitionBy("plan")
w4 = Window.partitionBy("customer_id")
w5 = Window.orderBy(F.col("customer_id_stddev_samp_next_months_start_date_to_end_date").asc_nulls_last())

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & F.col("status").isin(["PAUSED", "ACTIVE", "CANCELLED"]))
    .select(
        "*",
        F.round((F.datediff("end_date", "start_date") / 30), 1).alias("months_start_date_to_end_date"),
    )
)

scored = (
    valid
    .filter(F.col("status") == "ACTIVE")
    .withColumn("next_months_start_date_to_end_date", F.lead("months_start_date_to_end_date", 1).over(w_part))
    .withColumn("monthly_fee_bucket", F.ntile(5).over(w2))
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w3)), 4))
)

enriched = (
    scored
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.credit_limit"), F.col("d.age"))
)

remaining_keys = (
    enriched
    .select("customer_id", "plan")
    .exceptAll(
        df_subscriptions
        .filter(F.col("end_date") > "2024-01-31")
        .select("customer_id", "plan")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .select(
        "src.id",
        "src.customer_id",
        "src.start_date",
        "src.end_date",
        "src.next_months_start_date_to_end_date",
        "src.monthly_fee_share",
        "src.age",
        F.col("p.age").alias("customer_age"),
        F.col("p.credit_limit").alias("customer_credit_limit"),
    )
)

enriched_subscriptions = (
    with_dims
    .select(
        "id",
        "start_date",
        "next_months_start_date_to_end_date",
        "monthly_fee_share",
        F.stddev_samp("next_months_start_date_to_end_date").over(w4).alias("customer_id_stddev_samp_next_months_start_date_to_end_date"),
    )
)

metrics = (
    enriched_subscriptions
    .select(
        "start_date",
        "monthly_fee_share",
        "customer_id_stddev_samp_next_months_start_date_to_end_date",
        F.cume_dist().over(w5).alias("customer_id_stddev_samp_next_months_start_date_to_end_date_cume_dist"),
    )
)

result = (
    metrics
    .select(
        "start_date",
        "monthly_fee_share",
        "customer_id_stddev_samp_next_months_start_date_to_end_date",
        "customer_id_stddev_samp_next_months_start_date_to_end_date_cume_dist",
    )
)
