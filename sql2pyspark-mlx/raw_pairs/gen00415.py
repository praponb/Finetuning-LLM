from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") < "2024-09-12") & (F.col("delivered_date") < "2025-02-17"))
    .select("id", "status", F.upper("status").alias("status_upper"))
    .orderBy("id")
)
