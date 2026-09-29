from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id", F.month("delivered_date").alias("delivered_month"))
    .agg(
        F.count("*").alias("total_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy("status_values")
)
