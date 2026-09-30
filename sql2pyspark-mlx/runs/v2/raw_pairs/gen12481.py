from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id") == 16)
    .select("id", "unit_price", F.round("quantity", 2).alias("quantity_rounded"))
)
