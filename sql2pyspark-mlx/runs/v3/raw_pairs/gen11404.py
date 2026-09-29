from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNull() & F.col("order_id").isin([5, 11, 20, 38]))
    .select("carrier", "order_id")
    .orderBy(F.col("order_id").desc())
)
