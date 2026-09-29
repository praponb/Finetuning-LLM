from pyspark.sql import functions as F

recent_orders = (
    df_orders
    .filter((F.col("discount") > 0.16) & (F.col("discount") > 0.17) & (F.col("status") == "SHIPPED"))
)

summary = (
    recent_orders
    .groupBy("channel")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = summary.orderBy("channel")
