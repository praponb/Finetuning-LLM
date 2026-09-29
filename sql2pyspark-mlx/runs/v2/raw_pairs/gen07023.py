from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "LOST")
    .groupBy("carrier", "order_id")
    .agg(F.min("delivered_date").alias("first_delivered_date"))
    .orderBy("first_delivered_date", F.col("order_id").desc())
)
