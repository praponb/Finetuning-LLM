from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") >= 77) & F.col("delivered_date").between("2023-08-01", "2025-02-01"))
    .select("id", "delivered_date", "shipped_date")
)
