from pyspark.sql import functions as F

result = (
    df_shipments
    .select("id", "carrier", "cost", F.lower("status").alias("status_lower"))
)
