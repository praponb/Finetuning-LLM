from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") > "2025-01-01")
    .select("id", "status", "shipped_date")
)
