from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged") | F.col("merchant").isin(["Lazada", "Amazon", "Grab"]))
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .orderBy(F.col("stddev_amount").desc())
)
