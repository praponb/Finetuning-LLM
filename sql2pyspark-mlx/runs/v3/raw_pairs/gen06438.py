from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") <= 650)
    .groupBy(F.year("order_date").alias("order_year"), "status")
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
        F.min("order_date").alias("first_order_date"),
    )
    .orderBy("cancelled_count")
)
