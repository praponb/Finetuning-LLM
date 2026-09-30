from pyspark.sql import functions as F

result = (
    df_transactions
    .select("id", "amount", F.round("amount", 0).alias("amount_rounded"))
    .orderBy(F.col("id").desc(), "amount")
)
