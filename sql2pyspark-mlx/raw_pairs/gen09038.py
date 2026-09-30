from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") < 2000) & ~F.col("is_flagged") & F.col("is_flagged"))
    .select("is_flagged", F.round("amount", 1).alias("amount_rounded"))
)
