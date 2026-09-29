from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
    .filter(F.col("amount_range") < 49)
)
