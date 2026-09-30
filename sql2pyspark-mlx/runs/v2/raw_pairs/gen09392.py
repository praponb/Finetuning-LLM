from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.sum("discount").alias("total_discount"),
        F.count("*").alias("total_count"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("min_amount") >= 1200)
    .drop("min_amount")
    .orderBy(F.col("total_discount").desc(), "customer_id")
    .limit(5)
)
