from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2023-08-01", "2025-10-01"))
    .select("id", "shipped_date", F.upper("status").alias("status_upper"))
)
