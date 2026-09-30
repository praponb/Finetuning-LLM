from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost", "id")
w2 = Window.partitionBy("order_id", "status")
w3 = Window.orderBy(F.col("row_seq").asc_nulls_last())
w4 = Window.partitionBy("order_status", "status").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_status").orderBy("row_seq", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.partitionBy("order_id").orderBy(F.col("last_known_cost").desc(), "id")
w7 = Window.partitionBy("order_id", "status").orderBy(F.col("cost").desc(), "id")
w8 = Window.partitionBy(F.date_trunc("week", "shipped_date"), "order_id").orderBy("row_seq_cume_dist_avg_5", "id")
w9 = Window.partitionBy(F.date_trunc("month", "order_date"), "status")

enriched = (
    df_shipments
    .filter(F.col("shipped_date") > "2024-11-01")
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w2)), 4))
)

with_dims = (
    enriched
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id").eqNullSafe(F.col("p.id")), "inner")
    .select(
        "s.*",
        F.col("p.discount"),
        F.col("p.status").alias("order_status"),
        F.col("p.order_date"),
    )
)

enriched_shipments = (
    with_dims
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.order_date").alias("order_order_date"))
)

metrics = (
    enriched_shipments
    .select(
        "*",
        F.cume_dist().over(w3).alias("row_seq_cume_dist"),
        F.last("cost", ignorenulls=True).over(w4).alias("last_known_cost"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "order_date",
        "row_seq_cume_dist",
        F.avg("row_seq_cume_dist").over(w5).alias("row_seq_cume_dist_avg_5"),
        F.ntile(10).over(w6).alias("last_known_cost_bucket"),
    )
)

ranked = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    ranked
    .select(
        "id",
        "order_id",
        "shipped_date",
        "cost",
        "row_seq_cume_dist",
        "last_known_cost_bucket",
        "rank_in_group",
        F.ntile(4).over(w8).alias("row_seq_cume_dist_avg_5_bucket"),
        F.avg("row_seq_cume_dist_avg_5").over(w9).alias("order_date_period_avg_row_seq_cume_dist_avg_5"),
    )
)

result = (
    with_windows
    .groupBy("order_id")
    .agg(
        F.max("order_date_period_avg_row_seq_cume_dist_avg_5").alias("max_order_date_period_avg_row_seq_cume_dist_avg_5"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.round(F.avg("last_known_cost_bucket"), 2).alias("avg_last_known_cost_bucket"),
    )
)
