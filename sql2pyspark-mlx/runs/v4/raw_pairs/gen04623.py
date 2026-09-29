from pyspark.sql import functions as F

src = (
    df_orders
    .filter((F.col("order_date") < "2024-03-29") | F.col("shipping_fee").between(12, 19))
)

summary = (
    src
    .groupBy("channel", "status")
    .agg(F.sort_array(F.collect_set("channel")).alias("channel_list"))
)

result = summary.select("channel", "channel_list")
