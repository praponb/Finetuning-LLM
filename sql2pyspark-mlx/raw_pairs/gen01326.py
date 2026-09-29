from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("customer_id", "plan").orderBy(F.col("monthly_fee_whole").asc_nulls_first())
w3 = Window.orderBy(F.col("monthly_fee").desc_nulls_first())

cleaned = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select("*", F.ceil("monthly_fee").alias("monthly_fee_whole"))
)

kept_keys = (
    cleaned
    .select("id", "customer_id")
    .subtract(
        df_subscriptions
        .filter(F.col("status").isin(["ACTIVE", "PAUSED"]))
        .select("id", "customer_id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "customer_id",
        "monthly_fee",
        "status",
        "monthly_fee_whole",
        F.min("monthly_fee").over(w).alias("plan_min_monthly_fee"),
        F.dense_rank().over(w2).alias("monthly_fee_whole_dense_rank"),
    )
)

enriched_subscriptions = (
    enriched
    .select(
        "customer_id",
        "monthly_fee",
        "plan_min_monthly_fee",
        "monthly_fee_whole_dense_rank",
        F.cume_dist().over(w3).alias("monthly_fee_cume_dist"),
    )
)

result = (
    enriched_subscriptions
    .select(
        "customer_id",
        "monthly_fee_cume_dist",
        F.col("monthly_fee").alias("MONTHLY_FEE"),
    )
    .unpivot(
        ids=["customer_id"],
        values=["monthly_fee_cume_dist", "MONTHLY_FEE"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
