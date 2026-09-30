from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2023-10-12", "2024-05-01"))
    .select("account_type", "customer_id")
    .intersect(
        df_accounts
        .filter(F.col("status") == "OPEN")
        .select("account_type", "customer_id")
    )
    .orderBy("customer_id")
)
