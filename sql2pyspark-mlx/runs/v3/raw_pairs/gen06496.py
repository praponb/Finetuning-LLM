from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([7, 19, 24, 39]))
    .select("id", "status")
    .unionByName(
        df_accounts
        .filter(F.col("opened_date").between("2024-12-06", "2025-03-01"))
        .select("id", "status")
    )
    .distinct()
    .orderBy(F.col("status").desc())
)
