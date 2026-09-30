from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") < 120) & (F.col("unit_price") < 140))
    .select("id", "unit_price", "order_id")
    .orderBy(F.col("id").desc())
    .limit(1)
)
