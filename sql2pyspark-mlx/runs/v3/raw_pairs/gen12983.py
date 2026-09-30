from pyspark.sql import functions as F

result = (
    df_order_items
    .select("id", "unit_price", "order_id", "product_id")
    .orderBy(F.col("product_id").desc(), "id")
    .limit(3)
)
