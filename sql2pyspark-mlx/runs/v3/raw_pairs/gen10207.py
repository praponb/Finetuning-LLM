from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") > 22000) & ((F.col("status") == "FROZEN") | (F.col("balance") < 33000)))
    .select(
        "id",
        "customer_id",
        F.round((F.col("balance") * 1.15), 2).alias("balance_scaled"),
    )
)
