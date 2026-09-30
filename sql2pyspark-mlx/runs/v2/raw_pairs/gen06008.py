from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "OPEN")
    .select(
        "customer_id",
        "account_type",
        F.length("account_type").alias("account_type_length"),
        F.when(F.col("status") == "OPEN", 1).otherwise(0).alias("is_open"),
    )
    .orderBy("account_type")
)
