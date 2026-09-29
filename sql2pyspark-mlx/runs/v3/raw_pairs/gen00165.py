from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "balance",
        "opened_date",
        F.round((F.col("balance") * 0.85), 2).alias("balance_scaled"),
    )
)
