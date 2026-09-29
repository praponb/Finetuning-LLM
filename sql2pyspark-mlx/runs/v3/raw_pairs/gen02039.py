from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "DHL")
    .select("status", F.date_trunc("week", "shipped_date").alias("shipped_date_period"))
)
