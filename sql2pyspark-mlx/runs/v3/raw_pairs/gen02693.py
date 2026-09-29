from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([23, 25, 26, 29]) & (F.col("unit_price") < 240))
    .select("id", "order_id", "unit_price")
)
