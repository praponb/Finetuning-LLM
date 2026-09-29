from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("num_orders"),
        F.avg("discount").alias("avg_discount"),
    )
    .filter(F.col("avg_discount") < 0.27)
    .select("customer_id", "num_orders", "avg_discount")
)
