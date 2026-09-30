from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.year("order_date") >= 2025) & F.col("customer_id").isNull())
    .groupBy("status", F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy("status_values")
)
