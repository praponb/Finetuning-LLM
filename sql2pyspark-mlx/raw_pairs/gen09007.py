from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~((F.col("balance") <= 68000) | (F.col("opened_date") <= "2025-04-08")))
    .select(
        "id",
        "balance",
        F.round((F.col("balance") * 100), 2).alias("balance_adjusted"),
        F.round("balance", 1).alias("balance_rounded"),
    )
)
