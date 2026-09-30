from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([2, 4, 25, 33]) & (F.col("order_id") == 7))
    .select("id", "status", "order_id")
)
