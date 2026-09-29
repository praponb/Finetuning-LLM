from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("status").isin(["PAUSED", "ACTIVE"]), "tier_1").otherwise("other").alias("status_group"),
        F.quarter("start_date").alias("start_quarter"),
    )
    .agg(F.sum(F.when(F.col("plan") == "basic", 1).otherwise(0)).alias("basic_count"))
    .filter(F.col("basic_count") < 77)
)
