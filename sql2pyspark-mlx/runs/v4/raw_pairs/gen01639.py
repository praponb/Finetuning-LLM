from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("delivered_date").desc())
w2 = Window.partitionBy("order_status").orderBy(F.col("shipping_fee").desc())
w3 = Window.partitionBy("status").orderBy(F.col("cost").desc())
w_recent = Window.partitionBy("status").orderBy(F.col("shipping_fee").desc_nulls_last())

base = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select("*", F.substring("carrier", 1, 3).alias("carrier_prefix"))
)

enriched = (
    base
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .select("a.*", F.col("o.status").alias("order_status"), F.col("o.shipping_fee"))
)

latest = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    latest
    .select(
        "*",
        F.percent_rank().over(w2).alias("shipping_fee_percent_rank"),
        F.rank().over(w3).alias("cost_rank"),
        F.percent_rank().over(w_recent).alias("shipping_fee_percent_rank_2"),
    )
)

with_dims = (
    scored
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.channel"))
)

result = (
    with_dims
    .groupBy(F.date_trunc("quarter", "delivered_date").alias("quarter_start"))
    .agg(
        F.sort_array(F.collect_set("order_status")).alias("order_status_list"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.sum(F.when(F.col("status") == "IN_TRANSIT", F.col("shipping_fee_percent_rank"))).alias("in_transit_total_shipping_fee_percent_rank"),
        F.count_if(F.col("status") == "LOST").alias("lost_events"),
        F.count_if(F.col("channel") == "store").alias("store_events"),
    )
    .filter(F.col("avg_shipping_fee") > 0)
    .orderBy("in_transit_total_shipping_fee_percent_rank")
)
