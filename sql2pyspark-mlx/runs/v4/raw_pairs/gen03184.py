from pyspark.sql import functions as F

result = (
    df_accounts
    .select("account_type", "customer_id")
    .exceptAll(
        df_accounts
        .filter(F.col("account_type").isin(["CREDIT", "SAVINGS", "LOAN"]))
        .select("account_type", "customer_id")
    )
)
