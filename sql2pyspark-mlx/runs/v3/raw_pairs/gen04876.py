from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)
