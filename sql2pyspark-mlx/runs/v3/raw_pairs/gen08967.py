from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "balance",
        "customer_id",
        "account_type",
        F.when(F.col("status") == "FROZEN", 1).otherwise(0).alias("is_frozen"),
    )
    .orderBy(F.col("is_frozen").desc())
)
