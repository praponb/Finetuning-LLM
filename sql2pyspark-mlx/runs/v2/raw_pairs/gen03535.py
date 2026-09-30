from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sum("amount").alias("total_amount"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("total_amount") < 23000)
    .orderBy("merchant")
)
