from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2023-11-01", "2025-02-01"))
    .select("status")
    .unionByName(
        df_accounts
        .filter(F.col("opened_date").between("2025-08-24", "2025-11-01"))
        .select("status")
    )
    .distinct()
)
