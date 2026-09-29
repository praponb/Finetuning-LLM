from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").isNotNull())
    .select("status")
    .intersect(
        df_shipments
        .filter(F.col("delivered_date").between("2024-12-01", "2025-08-01"))
        .select("status")
    )
)
