from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") == 8)
    .select("id", "product_id", F.round("unit_price", 1).alias("unit_price_rounded"))
)
