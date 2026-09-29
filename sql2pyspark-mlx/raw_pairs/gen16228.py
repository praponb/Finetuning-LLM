from pyspark.sql import functions as F

result = (
    df_payments
    .select(F.lit("payment").alias("kind"), "method")
    .union(
        df_accounts
        .filter(F.col("account_type") == "CHECKING")
        .select(F.lit("account").alias("kind"), "account_type")
    )
    .distinct()
)
