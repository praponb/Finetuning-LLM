from pyspark.sql import functions as F

with_dims = (
    df_shipments
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.store_id").isin([22, 24]))
    .select("s.*", F.col("o.channel"))
)

result = (
    with_dims
    .withColumn("activity_month", F.date_trunc("MONTH", "delivered_date"))
    .rollup("activity_month")
    .agg(
        F.stddev_samp("cost").alias("cost_stddev"),
        F.count("*").alias("cnt"),
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.max(F.when(F.col("channel") == "web", F.col("cost"))).alias("web_max_cost"),
        F.count("*").alias("row_count"),
        F.grouping("activity_month").alias("grouping_level"),
    )
    .orderBy(F.col("row_count").desc_nulls_first())
)
