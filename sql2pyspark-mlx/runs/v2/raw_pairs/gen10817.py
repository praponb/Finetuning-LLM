from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("account_id") == 1) & (F.col("amount").between(2800, 3500) | F.col("account_id").isin([8, 19])))
    .select(
        "id",
        "account_id",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)
