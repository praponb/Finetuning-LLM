from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isNull() & (F.col("amount") > 3300))
    .groupBy("merchant")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
    .orderBy("merchant")
)
