from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([18, 25, 37]))
    .select("account_id")
    .unionByName(
        df_transactions
        .filter(F.col("txn_time") < "2024-12-31")
        .select("account_id")
    )
)
