from pyspark.sql import functions as F

result = (
    df_order_items
    .select("id", "product_id", "quantity", "order_id")
    .orderBy(F.col("id").desc(), F.col("quantity").desc())
    .limit(10)
)
