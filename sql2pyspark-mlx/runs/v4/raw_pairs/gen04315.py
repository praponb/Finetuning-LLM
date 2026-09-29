from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([6, 10, 17]) & (F.col("unit_price") > 220))
    .select("product_id", "order_id")
)
