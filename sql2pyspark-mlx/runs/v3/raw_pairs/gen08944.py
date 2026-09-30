from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2025-02-01", "2025-03-20"))
    .select(
        "id",
        "shipped_date",
        F.last_day("shipped_date").alias("shipped_date_month_end"),
    )
)
