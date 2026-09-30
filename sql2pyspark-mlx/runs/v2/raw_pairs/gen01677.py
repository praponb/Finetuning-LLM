from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") >= 1230)
    .groupBy("channel")
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("status") == "SHIPPED", F.col("amount")).otherwise(0)).alias("shipped_amount"),
    )
    .filter(F.col("shipped_amount") > 39)
    .drop("shipped_amount")
)
