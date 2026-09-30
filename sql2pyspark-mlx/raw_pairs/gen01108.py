from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([5, 12, 19, 22]))
    .select("balance", F.round("balance", 0).alias("balance_rounded"))
)
