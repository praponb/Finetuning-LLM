from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        F.round((F.col("balance") * 100), 2).alias("adjusted_balance"),
    )
)
