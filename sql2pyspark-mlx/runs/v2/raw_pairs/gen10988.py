from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") & ((F.col("merchant") == "Starbucks") | (F.col("amount") < 3600)))
    .groupBy("merchant")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .filter(F.col("stddev_amount") < 78)
)
