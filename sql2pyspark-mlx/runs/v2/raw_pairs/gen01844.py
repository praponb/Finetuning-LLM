from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([11, 33]) & F.col("quantity").between(3, 7) & (F.col("quantity") == 6))
    .select("id", "unit_price")
    .orderBy(F.col("id").desc())
    .limit(20)
)
