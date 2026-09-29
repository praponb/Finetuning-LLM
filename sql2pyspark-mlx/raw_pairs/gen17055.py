from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([10, 11, 17, 26]))
    .select("id", "order_id", "quantity")
    .orderBy(F.col("quantity").desc(), F.col("id").desc())
    .limit(1)
)
