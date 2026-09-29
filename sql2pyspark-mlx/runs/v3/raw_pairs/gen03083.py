from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") <= 1800)
    .select("account_id")
    .intersect(
        df_transactions
        .filter(F.col("account_id").isin([20, 23, 36, 39]))
        .select("account_id")
    )
)
