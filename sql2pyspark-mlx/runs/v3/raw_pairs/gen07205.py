from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "US")
    .select("city")
    .intersect(
        df_stores
        .filter(F.col("opened_date").between("2023-12-01", "2025-09-01"))
        .select("city")
    )
)
