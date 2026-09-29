from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(
        F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0)).alias("returned_count"),
        F.max("order_date").alias("latest_order_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("max_amount") >= 650)
    .drop("max_amount")
    .orderBy("order_month", F.col("latest_order_date").desc())
    .limit(5)
)
