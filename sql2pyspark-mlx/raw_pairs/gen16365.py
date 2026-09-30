from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull() & (F.col("unit_price") < 280))
    .select("id", "unit_price")
)
