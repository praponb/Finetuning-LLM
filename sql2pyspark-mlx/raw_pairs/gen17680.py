from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy("delivered_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("shipped_date").desc(), "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("cost").desc_nulls_last())
w4 = Window.orderBy(F.col("cost").asc_nulls_first())
w_running = Window.partitionBy("channel").orderBy("cost_percent_rank", "id")
w6 = Window.partitionBy("channel").orderBy(F.col("event_seq").desc_nulls_last())
w7 = Window.partitionBy("channel").orderBy(F.col("prev_cost_percent_rank").desc(), "id")
w8 = Window.partitionBy("channel").orderBy(F.col("store_events").desc())

cleaned = (
    df_shipments
    .filter((F.col("shipped_date") > F.to_date(F.lit("2023-08-09"))) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.add_months("delivered_date", 3).alias("delivered_date_plus_months"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("resolved_carrier"),
    )
)

enriched = (
    cleaned
    .withColumn("cost_avg_2", F.avg("cost").over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("cost_percent_rank", F.percent_rank().over(w3))
    .withColumn("cost_cume_dist", F.cume_dist().over(w4))
)

enriched_shipments = (
    enriched
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.order_id",
        "a.delivered_date_plus_months",
        "a.cost_percent_rank",
        F.col("d.channel"),
        F.col("d.amount"),
    )
)

scored = (
    enriched_shipments
    .select(
        "*",
        F.lag("cost_percent_rank").over(w_running).alias("prev_cost_percent_rank"),
        F.lag("amount").over(w_running).alias("prev_amount"),
        F.row_number().over(w_running).alias("event_seq"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "order_id",
        "delivered_date_plus_months",
        "channel",
        "prev_cost_percent_rank",
        "event_seq",
        F.rank().over(w6).alias("event_seq_rank"),
        F.ntile(4).over(w7).alias("prev_cost_percent_rank_bucket"),
    )
)

result = (
    with_windows
    .groupBy(
        "channel",
        F.date_trunc("YEAR", "delivered_date_plus_months").alias("year_start"),
    )
    .agg(
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_rows"),
        F.count_if(F.col("channel") == "store").alias("store_events"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("event_seq_rank") / F.nullif(F.count(F.when(F.col("channel") == "web", 1)), F.lit(0)), 2).alias("event_seq_rank_per_web"),
    )
    .withColumn("store_events_dense_rank", F.dense_rank().over(w8))
)
