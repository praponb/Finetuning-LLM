from pyspark.sql import functions as F

valid_orders = (
    df_orders
    .filter((F.col("order_date") >= "2025-02-01") & (F.col("order_date") >= "2024-01-10") & (F.col("status") == "RETURNED"))
)

agg = (
    valid_orders
    .groupBy("channel")
    .agg(F.sort_array(F.collect_set("channel")).alias("channel_list"))
)

result = agg
