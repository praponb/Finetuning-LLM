from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.min("order_date").alias("min_order_date"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
    )
    .orderBy(F.col("returned_count").desc(), "status")
    .limit(10)
)
