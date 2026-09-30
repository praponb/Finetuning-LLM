from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("carrier").orderBy("cost", "id")
w2 = Window.orderBy(F.col("cost").asc_nulls_first())
w3 = Window.partitionBy("status").orderBy("delivered_date")
w4 = Window.partitionBy("order_status")
w5 = Window.partitionBy("carrier", "order_id").orderBy(F.col("cost_bucket").asc_nulls_last())
w6 = Window.partitionBy("channel", "carrier")
w7 = Window.partitionBy("carrier").orderBy(F.col("cost_bucket").desc())

enriched = (
    df_shipments
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "status",
        F.ntile(3).over(w_recent).alias("cost_bucket"),
        F.percent_rank().over(w2).alias("cost_percent_rank"),
    )
)

deduped = (
    enriched
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
)

with_dims = (
    deduped
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "inner")
    .select(
        "a.id",
        "a.order_id",
        "a.carrier",
        "a.delivered_date",
        "a.status",
        "a.cost_bucket",
        "a.cost_percent_rank",
        F.col("b.channel"),
        F.col("b.discount"),
        F.col("b.status").alias("order_status"),
    )
)

matched_keys = (
    with_dims
    .select("order_id", "carrier")
    .subtract(
        df_shipments
        .filter(F.col("status") == "LOST")
        .select("order_id", "carrier")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

enriched_shipments = (
    verified
    .filter(F.col("channel") == "web")
    .select(
        "id",
        "order_id",
        "cost_bucket",
        F.max("discount").over(w4).alias("order_status_max_discount"),
        F.dense_rank().over(w5).alias("cost_bucket_dense_rank"),
        F.avg("discount").over(w6).alias("channel_avg_discount"),
        F.percent_rank().over(w7).alias("cost_bucket_percent_rank"),
    )
)

grouped = (
    enriched_shipments
    .groupBy("order_id")
    .agg(
        F.percentile_approx("order_status_max_discount", 0.95).alias("order_status_max_discount_p95"),
        F.count("*").alias("row_count"),
        F.percentile_approx("cost_bucket_dense_rank", [0.25, 0.5, 0.75]).alias("cost_bucket_dense_rank_quartiles"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.round(F.avg("order_status_max_discount"), 2).alias("avg_order_status_max_discount"),
        F.min("channel_avg_discount").alias("min_channel_avg_discount"),
    )
    .filter(F.col("distinct_order_ids") > 1)
)

result = (
    grouped
    .select(
        "order_id",
        "cost_bucket_dense_rank_quartiles",
        "distinct_order_ids",
        "avg_order_status_max_discount",
        "min_channel_avg_discount",
    )
    .orderBy(F.col("order_id").asc_nulls_last())
)
