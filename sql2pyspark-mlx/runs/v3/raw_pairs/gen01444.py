from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        "status",
        F.when(F.col("cost") >= 86, "high").when(F.col("cost") >= 32, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.max("cost").alias("max_cost"),
        F.max("shipped_date").alias("latest_shipped_date"),
    )
    .filter(F.col("max_cost") >= 36)
)
