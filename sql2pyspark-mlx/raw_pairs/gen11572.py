from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.count("*").alias("num_accounts"),
        F.count("*").alias("account_count"),
    )
    .filter(F.col("account_count") >= 2)
)
