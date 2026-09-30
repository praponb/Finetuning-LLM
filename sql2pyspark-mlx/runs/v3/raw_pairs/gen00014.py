from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") < 2000)
    .select("id", "account_id")
    .subtract(
        df_transactions
        .filter(F.col("account_id") == 15)
        .select("id", "account_id")
    )
)
