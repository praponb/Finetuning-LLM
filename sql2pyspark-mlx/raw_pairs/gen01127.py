from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([5, 17]))
    .groupBy("method")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("min_amount") > 1070)
    .select("method", "stddev_amount", "min_amount")
)
