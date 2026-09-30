from pyspark.sql import functions as F

filtered_orders = df_orders.filter(F.col("amount").between(480, 1040))

summary = (
    filtered_orders
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
    )
)

result = summary.select("status", "channel_values")
