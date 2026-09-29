from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("account_id") == 11) & (F.col("account_id") == 3))
    .select(
        "id",
        "is_flagged",
        "amount",
        F.round((F.col("amount") * 1.07), 2).alias("amount_scaled"),
    )
)
