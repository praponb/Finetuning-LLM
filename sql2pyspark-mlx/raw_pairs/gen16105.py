from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") >= "2025-07-01") & (F.col("balance") <= 40000) & F.col("status").isin(["FROZEN", "OPEN"]))
    .select(
        "id",
        "balance",
        F.round((F.col("balance") * 1.1), 2).alias("balance_scaled"),
    )
)
