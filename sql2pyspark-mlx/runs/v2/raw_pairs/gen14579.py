from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([29, 34, 38]))
    .select("amount", F.round("amount", 0).alias("amount_rounded"))
    .orderBy(F.col("amount").desc())
)
