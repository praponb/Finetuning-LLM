from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") < 240) & ((F.col("order_id") == 3) | F.col("product_id").isNotNull()))
    .select("quantity", "product_id")
)
