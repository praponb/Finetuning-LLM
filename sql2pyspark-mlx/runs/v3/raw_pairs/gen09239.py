from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") >= "2024-11-01") & (F.col("delivered_date") < "2025-01-01"))
    .withColumn("shipped_date_plus_months", F.add_months("shipped_date", 3))
)
