from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") > 6) & F.col("product_id").isin([24, 28, 31, 35]) & (F.col("product_id") == 14))
    .groupBy("product_id", "order_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
    .filter(F.col("quantity_range") >= 78)
)
