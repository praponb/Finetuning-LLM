from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") >= 21000) | (F.col("balance") <= 72000))
    .select("balance", F.weekofyear("opened_date").alias("opened_date_weekofyear"))
)
