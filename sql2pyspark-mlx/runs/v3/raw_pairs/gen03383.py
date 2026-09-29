from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("customer_id")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
    )
    .orderBy(F.col("avg_discount").desc())
)
