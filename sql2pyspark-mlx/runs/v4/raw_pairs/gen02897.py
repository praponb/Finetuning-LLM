from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNotNull() | (F.col("channel") != "web"))
    .groupBy("status")
    .agg(
        F.min("amount").alias("min_amount"),
        F.sum("discount").alias("total_discount"),
    )
    .filter(F.col("total_discount") >= 1.3)
    .drop("total_discount")
    .orderBy(F.col("min_amount").desc(), "status")
    .limit(10)
)
