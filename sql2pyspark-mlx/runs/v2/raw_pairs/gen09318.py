from pyspark.sql import functions as F

result = (
    df_orders
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        (F.max("discount") - F.min("discount")).alias("discount_range"),
    )
    .orderBy(F.col("avg_amount").desc(), F.col("discount_range").desc())
)
