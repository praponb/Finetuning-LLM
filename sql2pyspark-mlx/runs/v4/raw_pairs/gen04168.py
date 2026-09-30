from pyspark.sql import functions as F

result = (
    df_transactions
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
    .orderBy(F.col("amount_range").desc())
)
