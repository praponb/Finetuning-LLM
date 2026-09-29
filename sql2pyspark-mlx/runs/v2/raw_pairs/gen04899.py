from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "LOAN")
    .select("account_type", "opened_date")
    .subtract(
        df_accounts
        .filter(F.col("balance") > 77000)
        .select("account_type", "opened_date")
    )
)
