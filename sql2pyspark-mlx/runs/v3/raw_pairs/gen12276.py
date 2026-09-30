from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        "status",
        F.when(F.col("cost") >= 89, "high").when(F.col("cost") >= 35, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.min("delivered_date").alias("min_delivered_date"),
        F.round(((100 * F.sum(F.when(F.col("status") == "DELIVERED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_delivered"),
        F.min("cost").alias("min_cost"),
    )
    .orderBy(F.col("min_delivered_date").desc())
)
