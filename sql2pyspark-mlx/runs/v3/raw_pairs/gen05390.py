from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id", "product_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)
