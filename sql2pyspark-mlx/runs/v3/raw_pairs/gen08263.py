from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([7, 11]) & (F.col("unit_price") <= 270))
    .select("unit_price", "product_id")
    .orderBy(F.col("unit_price").desc())
)
