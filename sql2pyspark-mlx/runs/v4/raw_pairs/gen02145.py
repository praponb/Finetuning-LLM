from pyspark.sql import functions as F

src = (
    df_orders
    .filter(F.col("amount").between(940, 1400) & F.col("discount").between(0.15, 0.23))
    .withColumn("discount_rounded", F.round("discount", 2))
)

agg = (
    src
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = (
    agg
    .filter(F.col("avg_amount") > 32)
    .select("order_quarter", "channel_list", "avg_amount")
)
