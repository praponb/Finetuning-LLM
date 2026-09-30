from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNull() & (F.col("status") == "DELIVERED"))
    .withColumn("cost_adjusted", F.round((F.col("cost") * 0.85), 2))
)
