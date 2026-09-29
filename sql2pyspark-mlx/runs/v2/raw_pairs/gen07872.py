from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("shipped_date").isin([3, 9, 12]))
    .select("shipped_date", "status")
    .subtract(
        df_shipments
        .filter(F.col("shipped_date") < "2025-03-01")
        .select("shipped_date", "status")
    )
)
