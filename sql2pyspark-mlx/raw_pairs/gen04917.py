from pyspark.sql import functions as F

result = (
    df_orders
    .rollup("channel", "status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.max("order_date").alias("latest_order_date"),
    )
)
