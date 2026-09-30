from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") > 1120) & (F.col("method") == "bank_transfer"))
    .select("method", F.round("amount", 0).alias("amount_rounded"))
)
