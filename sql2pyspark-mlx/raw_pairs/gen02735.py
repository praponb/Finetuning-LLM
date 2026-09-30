from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        "status",
        F.when(F.col("discount") >= 0.35, "high").when(F.col("discount") >= 0.06, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg((F.max("discount") - F.min("discount")).alias("discount_range"))
    .filter(F.col("discount_range") < 31)
)
