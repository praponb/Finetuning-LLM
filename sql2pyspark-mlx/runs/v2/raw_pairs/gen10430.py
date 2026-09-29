from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([2, 29, 31]))
    .select("id", "unit_price")
    .orderBy(F.col("id").desc())
)
