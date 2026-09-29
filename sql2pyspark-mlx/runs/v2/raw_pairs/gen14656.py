from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~(F.col("order_id").isin([3, 20, 27, 28]) & F.col("order_id").isin([6, 26]) & (F.col("unit_price") <= 390)))
    .select("id", "quantity", "product_id")
)
