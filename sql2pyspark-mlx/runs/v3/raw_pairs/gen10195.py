from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 76, "high").when(F.col("cost") >= 44, "mid").otherwise("low").alias("cost_bucket"),
        "order_id",
    )
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
)
