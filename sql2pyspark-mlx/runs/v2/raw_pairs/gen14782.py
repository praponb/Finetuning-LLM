from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(1700, 2900))
    .select("id", "account_id")
    .unionByName(
        df_transactions
        .filter(F.col("account_id").isin([3, 5, 17, 22]))
        .select("id", "account_id")
    )
)
