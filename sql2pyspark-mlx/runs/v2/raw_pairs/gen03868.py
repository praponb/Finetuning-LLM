from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") > 2400)
    .select("id", "account_id")
    .unionByName(
        df_transactions
        .filter(F.col("account_id").isin([8, 10, 15]))
        .select("id", "account_id")
    )
)
