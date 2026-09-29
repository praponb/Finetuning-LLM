from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") != "CREDIT")
    .select(
        "account_type",
        F.length("status").alias("status_length"),
        F.date_sub("opened_date", 83).alias("opened_date_minus_83d"),
    )
)
