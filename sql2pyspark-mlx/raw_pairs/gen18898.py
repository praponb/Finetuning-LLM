from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        F.when(F.col("status") == "CLOSED", 1).otherwise(0).alias("is_closed"),
    )
)
