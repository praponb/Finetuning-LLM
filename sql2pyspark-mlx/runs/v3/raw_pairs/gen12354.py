from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged") & F.col("account_id").isin([23, 25, 28]))
    .select("id", "is_flagged", F.substring("merchant", 1, 2).alias("merchant_prefix"))
)
