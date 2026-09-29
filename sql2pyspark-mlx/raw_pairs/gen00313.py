from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .orderBy("stddev_amount")
)
