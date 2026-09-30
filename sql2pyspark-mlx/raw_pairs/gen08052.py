from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") < "2024-12-06")
    .select("shipped_date", "carrier")
    .intersect(
        df_shipments
        .filter(F.col("cost") < 91)
        .select("shipped_date", "carrier")
    )
)
