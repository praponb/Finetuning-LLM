from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").isNotNull() & F.col("product_id").isNotNull())
)
