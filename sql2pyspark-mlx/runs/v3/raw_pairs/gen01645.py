from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status", F.month("order_date").alias("order_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.max("order_date").alias("last_order_date"),
    )
    .orderBy(F.col("status_values").desc())
)
