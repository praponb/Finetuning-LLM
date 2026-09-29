from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method", "order_id")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("row_count").desc())
)
