from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.status"), F.col("o.channel"))
    .agg(
        F.sort_array(F.collect_set(F.col("s.status"))).alias("status_list"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.status")))).alias("status_values"),
    )
)
