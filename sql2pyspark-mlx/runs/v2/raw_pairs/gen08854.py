from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
    .filter(F.col("quantity_range") >= 49)
    .orderBy("order_id")
)
