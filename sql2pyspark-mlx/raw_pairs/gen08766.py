from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "carrier").orderBy("shipped_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("last7_avg_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("last7_sum_cost", "id")
w_recent = Window.partitionBy("carrier").orderBy("cost", "id")
w_grp = Window.orderBy(F.col("cumulative_last7_max_cost").asc_nulls_first())

enriched = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.order_date"))
)

with_windows = (
    enriched
    .filter(F.year("delivered_date") >= 2024)
    .select(
        "*",
        F.max("cost").over(w).alias("last7_max_cost"),
        F.sum("cost").over(w).alias("last7_sum_cost"),
        F.avg("cost").over(w).alias("last7_avg_cost"),
        F.count("cost").over(w).alias("last7_count_cost"),
    )
)

metrics = (
    with_windows
    .withColumn("cumulative_last7_max_cost", F.sum("last7_max_cost").over(w2))
    .withColumn("prev_last7_count_cost", F.lag("last7_count_cost", 2).over(w3))
)

enriched_shipments = (
    metrics
    .withColumn("prev_prev_last7_count_cost", F.lag("prev_last7_count_cost").over(w_recent))
    .withColumn("next_prev_last7_count_cost", F.lead("prev_last7_count_cost").over(w_recent))
    .withColumn("next_last7_count_cost", F.lead("last7_count_cost").over(w_recent))
    .withColumn("row_seq", F.row_number().over(w_recent))
    .withColumn("cumulative_last7_max_cost_cume_dist", F.cume_dist().over(w_grp))
)

with_dims = (
    enriched_shipments
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .select(
        "src.id",
        "src.order_id",
        "src.carrier",
        "src.shipped_date",
        "src.delivered_date",
        "src.last7_sum_cost",
        "src.last7_count_cost",
        "src.cumulative_last7_max_cost",
        "src.prev_prev_last7_count_cost",
        "src.next_prev_last7_count_cost",
        F.col("d.order_date").alias("order_order_date"),
        F.col("d.amount"),
    )
)

grouped = (
    with_dims
    .withColumn("last7_sum_cost_tier", F.when(F.col("last7_sum_cost") >= 400, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["last7_sum_cost_tier", "order_id", "carrier"], ["last7_sum_cost_tier", "order_id"], ["last7_sum_cost_tier"]],
        "last7_sum_cost_tier",
        "order_id",
        "carrier",
    )
    .agg(
        F.percentile_approx("next_prev_last7_count_cost", 0.5).alias("next_prev_last7_count_cost_p50"),
        F.avg(F.when(F.col("carrier") == "FedEx", F.col("prev_prev_last7_count_cost"))).alias("fedex_avg_prev_prev_last7_count_cost"),
    )
)

result = (
    grouped
    .select("order_id", "carrier", "next_prev_last7_count_cost_p50")
    .orderBy(F.col("carrier").desc_nulls_first())
)
