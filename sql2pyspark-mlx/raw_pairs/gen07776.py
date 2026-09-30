from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").isNotNull())
    .select("id", "order_id", "unit_price")
    .orderBy(F.col("order_id").desc())
)
