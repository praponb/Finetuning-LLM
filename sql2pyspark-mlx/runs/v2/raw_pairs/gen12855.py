from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~(F.col("shipped_date").between("2024-04-01", "2025-09-01") & (F.col("cost") < 64)))
    .select(
        "id",
        "carrier",
        "delivered_date",
        F.quarter("shipped_date").alias("shipped_date_quarter"),
        F.month("shipped_date").alias("shipped_date_month"),
    )
)
