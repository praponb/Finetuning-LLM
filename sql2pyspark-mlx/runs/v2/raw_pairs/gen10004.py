from pyspark.sql import functions as F

result = (
    df_order_items
    .select("id", "product_id", "order_id")
    .orderBy(F.col("order_id").desc())
)
