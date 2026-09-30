from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([33, 37]))
    .select("order_id", F.length("carrier").alias("carrier_length"))
)
