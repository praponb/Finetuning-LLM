from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2023-12-01", "2025-07-01"))
    .select("delivered_date", "shipped_date")
    .intersect(
        df_shipments
        .filter(F.col("shipped_date") > "2023-11-04")
        .select("delivered_date", "shipped_date")
    )
)
