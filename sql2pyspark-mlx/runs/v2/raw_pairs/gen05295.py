from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("shipped_date").isin([3, 4, 5]) & (F.col("cost") < 47) & F.col("delivered_date").between("2024-09-27", "2025-08-01"))
    .select("id", "cost", "shipped_date")
)
