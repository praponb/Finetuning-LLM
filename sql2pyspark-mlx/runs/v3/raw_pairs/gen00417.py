from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.year("order_date").alias("order_year"))
    .agg(
        F.sum(F.when(F.col("status") == "SHIPPED", F.col("amount")).otherwise(0)).alias("shipped_amount"),
    )
    .filter(F.col("shipped_amount") >= 27)
)
