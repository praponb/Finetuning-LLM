from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)
