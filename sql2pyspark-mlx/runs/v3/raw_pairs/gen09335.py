from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull() | (F.col("unit_price") < 320))
    .select("quantity", F.round("quantity", 2).alias("quantity_rounded"))
)
