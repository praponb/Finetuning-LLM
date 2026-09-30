from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") <= 30)
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("N/A")))
    .orderBy("shipped_date")
)
