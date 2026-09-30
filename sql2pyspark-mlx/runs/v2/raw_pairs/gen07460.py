from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") >= 3) & (F.col("unit_price") <= 250))
    .select("unit_price")
)
