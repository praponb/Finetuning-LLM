from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("shipped_date")).orderBy("cost", "id")
w2 = Window.orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("carrier", "channel")
w5 = Window.partitionBy("status").orderBy("carrier_sum_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("carrier_sum_cost", "id").rowsBetween(-2, -1)
w7 = Window.partitionBy("order_id", "status").orderBy(F.col("running_sum_carrier_sum_cost").desc())
w8 = Window.partitionBy("order_id").orderBy("running_min_carrier_sum_cost", "id")
w9 = Window.partitionBy("order_id").orderBy(F.col("running_sum_carrier_sum_cost").desc(), "id")

joined = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id").eqNullSafe(F.col("b.id")), "inner")
    .select("e.*", F.col("b.channel"))
)

metrics = (
    joined
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("cumulative_cost", F.sum("cost").over(w2))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w3)), 4))
    .withColumn("carrier_sum_cost", F.sum("cost").over(w4))
)

cleaned = (
    metrics
    .filter(F.col("delivered_date") > F.to_date(F.lit("2023-05-09")))
    .select(
        "id",
        "order_id",
        "cost",
        "status",
        "channel",
        "row_seq",
        "cumulative_cost",
        "carrier_sum_cost",
        F.upper("channel").alias("channel_upper"),
        (F.datediff(F.current_date(), "shipped_date") / 7).alias("weeks_since_shipped_date"),
        F.coalesce("carrier", F.lit("OTHER")).alias("resolved_carrier"),
    )
)

enriched = (
    cleaned
    .filter(F.col("order_id").isin([4, 27, 36, 37]))
    .withColumn("running_sum_carrier_sum_cost", F.sum("carrier_sum_cost").over(w5))
    .withColumn("running_count_carrier_sum_cost", F.count("carrier_sum_cost").over(w5))
    .withColumn("running_avg_carrier_sum_cost", F.avg("carrier_sum_cost").over(w5))
    .withColumn("running_min_carrier_sum_cost", F.min("carrier_sum_cost").over(w5))
    .withColumn("cost_avg_2_prior", F.avg("cost").over(w6))
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w7))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    deduped
    .withColumn("next_running_min_carrier_sum_cost", F.lead("running_min_carrier_sum_cost").over(w8))
    .withColumn("prev_running_min_carrier_sum_cost", F.lag("running_min_carrier_sum_cost").over(w8))
    .withColumn("next_cost_avg_2_prior", F.lead("cost_avg_2_prior").over(w8))
    .withColumn("prev_cost_avg_2_prior", F.lag("cost_avg_2_prior").over(w8))
    .withColumn("seq_no", F.row_number().over(w8))
    .withColumn("running_sum_carrier_sum_cost_bucket", F.ntile(3).over(w9))
)

result = (
    scored
    .withColumn("running_sum_carrier_sum_cost_tier", F.when(F.col("running_sum_carrier_sum_cost") >= 2040, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["order_id", "channel", "running_sum_carrier_sum_cost_tier"], ["order_id", "channel"], ["order_id"], []],
        "order_id",
        "channel",
        "running_sum_carrier_sum_cost_tier",
    )
    .agg(
        F.sum(F.when(F.col("channel") == "web", F.col("carrier_sum_cost")).otherwise(0)).alias("web_carrier_sum_cost"),
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_rows"),
    )
)
