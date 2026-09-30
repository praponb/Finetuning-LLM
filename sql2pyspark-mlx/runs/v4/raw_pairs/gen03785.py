from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 200, "LARGE").otherwise("SMALL").alias("monthly_fee_tier"),
        "status",
    )
    .agg(
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
        F.countDistinct("status").alias("distinct_status"),
    )
    .orderBy(F.col("distinct_status").desc())
)
