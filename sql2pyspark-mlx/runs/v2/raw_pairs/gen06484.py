from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id") == 5)
    .select("id", "carrier", "order_id", F.col("cost").cast("int").alias("cost_int"))
)
