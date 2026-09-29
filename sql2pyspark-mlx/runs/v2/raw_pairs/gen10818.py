from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~(((F.col("product_id") == 2) & F.col("order_id").isin([6, 22, 32, 35])) | (F.col("order_id") == 2)))
    .groupBy("product_id", "order_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)
