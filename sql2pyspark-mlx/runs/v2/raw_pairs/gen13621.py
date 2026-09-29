from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") > 320) & (F.col("product_id") == 35) & (F.col("unit_price") >= 270))
    .select("id", "product_id", "quantity", "unit_price")
)
