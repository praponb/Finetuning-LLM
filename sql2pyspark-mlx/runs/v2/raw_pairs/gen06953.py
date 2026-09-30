from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "account_type",
        "customer_id",
        F.when(F.col("balance") >= 79000, "High").when(F.col("balance") >= 31000, "Medium").otherwise("Low").alias("balance_level"),
    )
)
