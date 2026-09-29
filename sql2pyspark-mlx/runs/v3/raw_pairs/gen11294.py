from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").between("2024-09-01", "2025-07-01") & F.col("status").isin(["CANCELLED", "SHIPPED"]))
    .groupBy("channel")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("max_amount") >= 1280)
    .orderBy("max_amount")
)
