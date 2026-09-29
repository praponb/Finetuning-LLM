from pyspark.sql import functions as F

base = (
    df_orders
    .filter((F.col("amount") >= 1570) | F.col("status").isin(["RETURNED", "SHIPPED", "COMPLETED"]))
    .withColumn("shipping_fee_whole", F.floor("shipping_fee"))
)

order_month_summary = (
    base
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(
        F.count("*").alias("total_count"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = order_month_summary.orderBy(F.col("total_count").desc(), "order_month")
