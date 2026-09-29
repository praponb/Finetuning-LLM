from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([32, 36, 38]))
    .groupBy(
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
    .filter(F.col("quantity_range") > 45)
)
