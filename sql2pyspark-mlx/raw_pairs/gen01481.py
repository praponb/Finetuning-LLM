from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~(F.col("shipped_date").between("2023-09-01", "2025-04-01") & (F.col("shipped_date") > "2024-01-08")))
    .select("carrier", "delivered_date")
)
