from pyspark.sql import functions as F

filtered_orders = df_orders.filter(F.col("customer_id").isin([15, 31, 34, 37]))

summary = (
    filtered_orders
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    summary
    .select("status", "status_values")
    .orderBy(F.col("status").desc(), "status_values")
)
