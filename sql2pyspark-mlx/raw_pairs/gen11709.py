from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("carrier")
w_grp = Window.partitionBy("status", "carrier").orderBy("cost", "id")
w3 = Window.orderBy(F.col("cost").desc(), "id")
w4 = Window.partitionBy("order_status", "status").orderBy("order_date", "id")
w5 = Window.partitionBy(F.date_trunc("WEEK", "order_date"))
w6 = Window.orderBy("prev_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.date_trunc("YEAR", "order_date"), "order_status")
w8 = Window.orderBy(F.col("seq_no").desc_nulls_last())
w_recent = Window.partitionBy("status").orderBy("seq_no_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("status").orderBy(F.col("max_running_count_seq_no_rank").desc())

with_windows = (
    df_shipments
    .withColumn("carrier_stddev_samp_cost", F.stddev_samp("cost").over(w_running))
    .withColumn("cost_bucket", F.ntile(4).over(w_grp))
    .withColumn("cost_bucket_2", F.ntile(4).over(w3))
)

enriched = (
    with_windows
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.cost",
        "src.cost_bucket_2",
        F.col("p.order_date"),
        F.col("p.amount"),
        F.col("p.status").alias("order_status"),
    )
)

enriched_shipments = (
    enriched
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.channel") != "web")
    .select(
        "src.id",
        "src.order_id",
        "src.cost",
        "src.cost_bucket_2",
        "src.order_date",
        "src.amount",
        "src.order_status",
        F.col("o.shipping_fee"),
        F.col("o.status"),
    )
)

scored = (
    enriched_shipments
    .select(
        "id",
        "order_date",
        "amount",
        "order_status",
        "status",
        F.lag("amount").over(w4).alias("prev_amount"),
        F.lag("cost").over(w4).alias("prev_cost"),
        F.lead("cost").over(w4).alias("next_cost"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

with_windows_shipments = (
    scored
    .select(
        "id",
        "amount",
        "status",
        "prev_cost",
        "seq_no",
        F.round((F.col("seq_no") / F.sum("seq_no").over(w5)), 4).alias("seq_no_share"),
        F.sum("prev_amount").over(w6).alias("cumulative_prev_amount"),
        F.round((F.col("seq_no") / F.sum("seq_no").over(w7)), 4).alias("seq_no_share_2"),
        F.rank().over(w8).alias("seq_no_rank"),
    )
)

metrics = (
    with_windows_shipments
    .select(
        "id",
        "status",
        "prev_cost",
        "seq_no_share",
        "seq_no_share_2",
        F.avg("seq_no_rank").over(w_recent).alias("running_avg_seq_no_rank"),
        F.max("seq_no_rank").over(w_recent).alias("running_max_seq_no_rank"),
        F.count("seq_no_rank").over(w_recent).alias("running_count_seq_no_rank"),
        F.sum("seq_no_rank").over(w_recent).alias("running_sum_seq_no_rank"),
    )
)

result = (
    metrics
    .groupBy(
        "status",
        F.when(F.col("seq_no_share") >= 0.83, "PREMIUM").otherwise("STANDARD").alias("seq_no_share_tier"),
    )
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.count("*").alias("num_rows"),
        F.max("running_count_seq_no_rank").alias("max_running_count_seq_no_rank"),
    )
    .withColumn("max_running_count_seq_no_rank_percent_rank", F.percent_rank().over(w10))
    .orderBy(F.col("seq_no_share_tier").asc_nulls_first())
)
