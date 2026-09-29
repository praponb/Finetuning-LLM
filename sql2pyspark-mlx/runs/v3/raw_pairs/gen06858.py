from pyspark.sql import functions as F

result = (
    df_order_items
    .select("id", "quantity", "unit_price", "product_id")
    .orderBy(F.col("id").desc(), F.col("unit_price").desc())
)
