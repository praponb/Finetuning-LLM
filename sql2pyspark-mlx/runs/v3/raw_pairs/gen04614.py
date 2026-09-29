from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.year("shipped_date") == 2025) & F.col("delivered_date").between("2024-02-01", "2024-11-12"))
    .select("id", "cost", "status", "carrier", F.trim("carrier").alias("clean_carrier"))
)
