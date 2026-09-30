from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "CHECKING")
    .select("id", "account_type", "customer_id")
    .intersect(
        df_accounts
        .filter(F.col("account_type") == "SAVINGS")
        .select("id", "account_type", "customer_id")
    )
    .orderBy(F.col("account_type").desc())
)
