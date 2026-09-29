from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("channel").orderBy("shipped_date", "id")
w2 = Window.partitionBy("status", "order_id").orderBy("discount", "id")
w3 = Window.partitionBy("order_id", "status").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status", "channel").orderBy("running_sum_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("prev_cost", "id")
w6 = Window.partitionBy(F.date_trunc("month", "delivered_date"), "status").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("channel").orderBy("prev_discount_2", "id")
w8 = Window.partitionBy("channel").orderBy("running_count_cost", "id")
w9 = Window.partitionBy(F.date_trunc("WEEK", "delivered_date"), "channel").orderBy("running_avg_cost", "id").rowsBetween(-3, -1)
w10 = Window.partitionBy("channel", "status").orderBy("prev_discount_2", "id")
w11 = Window.partitionBy("status")
w12 = Window.partitionBy("status").orderBy("next_prev_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.partitionBy("channel")
w14 = Window.partitionBy("status").orderBy(F.col("avg_running_count_cost").desc())

valid = (
    df_shipments
    .filter(F.month("shipped_date").isin([2, 4, 5]))
    .withColumn("status_prefix", F.substring("status", 1, 3))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("UNKNOWN")))
)

with_dims = (
    valid
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id").eqNullSafe(F.col("o.id")), "inner")
    .select("s.*", F.col("o.order_date"), F.col("o.channel"), F.col("o.discount"))
)

metrics = (
    with_dims
    .select(
        "*",
        F.lag("cost").over(w_hist).alias("prev_cost"),
        F.lead("cost").over(w_hist).alias("next_cost"),
        F.lag("discount").over(w_hist).alias("prev_discount"),
        F.lead("discount").over(w_hist).alias("next_discount"),
        F.row_number().over(w_hist).alias("seq_no"),
        F.lag("discount", 1).over(w2).alias("prev_discount_2"),
    )
)

scored = (
    metrics
    .filter(F.col("seq_no") <= 7)
    .select(
        "id",
        "delivered_date",
        "status",
        "carrier_filled",
        "channel",
        "prev_cost",
        "prev_discount",
        "next_discount",
        "seq_no",
        "prev_discount_2",
        F.count("cost").over(w3).alias("running_count_cost"),
        F.sum("cost").over(w3).alias("running_sum_cost"),
        F.avg("cost").over(w3).alias("running_avg_cost"),
    )
)

enriched = (
    scored
    .withColumn("running_running_sum_cost", F.sum("running_sum_cost").over(w4))
    .withColumn("prev_cost_bucket", F.ntile(3).over(w5))
    .withColumn("cumulative_running_count_cost", F.sum("running_count_cost").over(w6))
    .withColumn("next_prev_cost", F.lead("prev_cost", 1).over(w7))
    .withColumn("seq_no_2", F.row_number().over(w8))
)

metrics_shipments = (
    enriched
    .select(
        "id",
        "status",
        "carrier_filled",
        "channel",
        "prev_discount",
        "next_discount",
        "seq_no",
        "running_count_cost",
        "seq_no_2",
        F.avg("next_discount").over(w9).alias("next_discount_avg_3_prior"),
        F.ntile(3).over(w10).alias("prev_discount_2_bucket"),
        F.round((F.col("next_prev_cost") / F.sum("next_prev_cost").over(w11)), 4).alias("next_prev_cost_share"),
        F.first("running_count_cost", ignorenulls=True).over(w12).alias("first_known_running_count_cost"),
        F.round((F.col("next_prev_cost") / F.sum("next_prev_cost").over(w13)), 4).alias("next_prev_cost_share_2"),
    )
)

result = (
    metrics_shipments
    .groupBy(
        "status",
        F.when(F.col("prev_discount") >= 0.39, "PREMIUM").otherwise("STANDARD").alias("prev_discount_tier"),
        F.when(F.col("running_count_cost") >= 13, "LARGE").otherwise("SMALL").alias("running_count_cost_tier"),
    )
    .agg(
        F.avg("running_count_cost").alias("avg_running_count_cost"),
        F.round(F.avg("next_prev_cost_share_2"), 2).alias("avg_next_prev_cost_share_2"),
        F.round(F.sum("running_count_cost") / F.nullif(F.count(F.when(F.col("status") == "LOST", 1)), F.lit(0)), 2).alias("running_count_cost_per_lost"),
    )
    .filter(F.col("avg_next_prev_cost_share_2") >= 0)
    .withColumn("avg_running_count_cost_dense_rank", F.dense_rank().over(w14))
    .orderBy(F.col("running_count_cost_per_lost").asc_nulls_last())
)
