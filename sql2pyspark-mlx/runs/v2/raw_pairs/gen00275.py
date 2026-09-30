from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select("order_id", F.lower("carrier").alias("carrier_lower"))
    .orderBy(F.col("carrier_lower").desc(), F.col("order_id").desc())
)
