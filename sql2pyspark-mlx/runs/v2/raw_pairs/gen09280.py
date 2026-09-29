from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("shipped_date") >= "2024-10-16") | (F.col("cost") > 34))
    .select("status", F.length("carrier").alias("carrier_length"))
)
