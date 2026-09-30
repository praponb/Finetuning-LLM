from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") < 200)
    .select("id", "order_id", "unit_price")
    .orderBy("id", F.col("order_id").desc())
)
