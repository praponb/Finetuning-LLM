from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~(F.month("shipped_date").isin([2, 3, 10]) & F.col("shipped_date").between("2025-07-01", "2025-09-01")))
    .select(
        "id",
        "status",
        "carrier",
        "order_id",
        F.lower("status").alias("status_lower"),
    )
)
