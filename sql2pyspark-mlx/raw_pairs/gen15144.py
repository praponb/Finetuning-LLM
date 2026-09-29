from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([7, 21, 23]) & F.col("order_id").isin([3, 25, 32, 33]) & (F.col("unit_price") > 340))
    .select("id", "quantity", "unit_price")
    .orderBy(F.col("unit_price").desc(), F.col("id").desc())
)
