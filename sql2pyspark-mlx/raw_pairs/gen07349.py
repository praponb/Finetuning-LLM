from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 223, "LARGE").otherwise("SMALL").alias("monthly_fee_tier"),
    )
    .agg(
        F.count_if(F.col("status") == "EXPIRED").alias("expired_events"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
    )
    .orderBy(F.col("monthly_fee_tier").desc())
)
