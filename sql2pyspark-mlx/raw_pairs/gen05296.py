from pyspark.sql import functions as F

valid_orders = (
    df_orders
    .filter((F.col("channel") == "partner") & ((F.col("shipping_fee") > 17) | ~F.col("status").isin(["SHIPPED", "COMPLETED", "RETURNED"])))
)

orders_stats = (
    valid_orders
    .groupBy("channel")
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    orders_stats
    .filter(F.col("min_amount") >= 1560)
    .orderBy(F.col("min_amount").desc())
)
