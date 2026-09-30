from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id") == 39)
    .groupBy("product_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
    .filter(F.col("quantity_range") >= 76)
    .orderBy(F.col("product_id").desc(), F.col("quantity_range").desc())
)
