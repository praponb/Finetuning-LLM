from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull() | (F.col("unit_price") > 190))
    .select("id", "order_id")
)
