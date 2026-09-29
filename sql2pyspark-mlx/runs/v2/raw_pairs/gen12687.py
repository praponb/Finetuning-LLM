from pyspark.sql import functions as F

recent_orders = df_orders.filter(F.col("amount") <= 1030)

agg = (
    recent_orders
    .groupBy("status", "channel")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0)).alias("partner_count"),
    )
)

result = (
    agg
    .filter(F.col("partner_count") > 69)
    .orderBy("status")
)
