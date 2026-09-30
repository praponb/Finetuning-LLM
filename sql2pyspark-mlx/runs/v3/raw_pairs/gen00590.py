from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2023-11-01", "2025-05-02"))
    .select("customer_id")
    .intersect(
        df_accounts
        .filter(F.col("opened_date") <= "2023-07-01")
        .select("customer_id")
    )
)
