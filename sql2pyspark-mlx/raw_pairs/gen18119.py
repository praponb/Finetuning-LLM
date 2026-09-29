from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.max("discount").alias("max_discount"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("max_amount") > 580)
    .drop("max_amount")
    .orderBy(F.col("channel").desc())
)
