from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "customer_id",
        "account_type",
        F.when(F.col("balance") >= 78000, "High").when(F.col("balance") >= 14000, "Medium").otherwise("Low").alias("balance_level"),
    )
)
