from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & (F.col("status") == "DELIVERED"))
    .select("cost", F.col("cost").cast("int").alias("cost_int"))
    .orderBy("cost_int")
)
