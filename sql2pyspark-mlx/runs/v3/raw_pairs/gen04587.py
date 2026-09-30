from pyspark.sql import functions as F

result = (
    df_transactions
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.count("*").alias("cnt"),
    )
)
