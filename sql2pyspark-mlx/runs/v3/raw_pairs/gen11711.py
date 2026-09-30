from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 89, "high").when(F.col("cost") >= 41, "mid").otherwise("low").alias("cost_bucket"),
        "status",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "Flash", 1).otherwise(0))) / F.count("*")), 1).alias("pct_flash"),
        F.count("cost").alias("cost_count"),
    )
)
