from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .select("id", "opened_date")
    .intersect(
        df_accounts
        .filter(F.col("opened_date").between("2024-11-01", "2025-06-11"))
        .select("id", "opened_date")
    )
)
