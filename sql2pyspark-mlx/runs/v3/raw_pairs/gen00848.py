from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") < 3)
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
    .orderBy("quantity_bucket")
)
