from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count("shipping_fee").alias("shipping_fee_count"),
    )
    .filter(F.col("shipping_fee_count") >= 5)
    .drop("shipping_fee_count")
    .orderBy("order_month")
    .limit(3)
)
