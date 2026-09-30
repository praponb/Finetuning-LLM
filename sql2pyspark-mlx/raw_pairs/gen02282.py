from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") > 100) & (F.col("quantity") <= 4) & F.col("order_id").isin([20, 25]))
    .select("unit_price", "order_id")
)
