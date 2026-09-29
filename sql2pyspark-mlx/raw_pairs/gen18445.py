from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("customer_id", "status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.sum("discount").alias("total_discount"),
    )
)
