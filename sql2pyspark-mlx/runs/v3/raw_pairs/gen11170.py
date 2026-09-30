from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").isNull() & (F.col("order_id") == 18))
    .select("id", "carrier", F.length("status").alias("status_length"))
)
