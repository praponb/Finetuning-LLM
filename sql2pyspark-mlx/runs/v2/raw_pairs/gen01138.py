from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2024-05-10", "2025-01-09"))
    .select("status")
    .unionByName(
        df_payments
        .filter(F.to_date("paid_at") >= "2023-10-01")
        .select("status")
    )
    .distinct()
)
