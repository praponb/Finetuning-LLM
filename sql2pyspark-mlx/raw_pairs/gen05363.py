from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("customer_id").isin([3, 29]) & (F.col("customer_id") == 34) & F.col("customer_id").isin([12, 20, 25, 34]))
)

order_month_summary = (
    base
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.max("order_date").alias("last_order_date"),
    )
)

result = (
    order_month_summary
    .orderBy("order_month")
    .limit(1)
)
