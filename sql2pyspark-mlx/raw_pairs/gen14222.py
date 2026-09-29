from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("island_id").desc(), "id")
w4 = Window.partitionBy("order_id").orderBy("island_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("method").orderBy(F.col("first_known_amount").desc_nulls_last())
w7 = Window.partitionBy("status", "method").orderBy(F.col("running_avg_amount").desc_nulls_last())
w8 = Window.partitionBy("order_id")
w9 = Window.orderBy(F.col("island_id").desc())
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.sum("cost").alias("sum_shipment_cost"))

base = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (~F.col("method").eqNullSafe(F.lit("cash"))))
    .select(
        "*",
        F.substring("method", 1, 3).alias("method_prefix"),
        F.coalesce("method", F.lit("OTHER")).alias("method_filled"),
    )
)

prev_events = base.withColumn("prev_paid_at", F.lag("paid_at").over(w))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

latest = (
    sessions_payments
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        "method_filled",
        "island_id",
    )
)

enriched = (
    latest
    .select(
        "*",
        F.min("amount").over(w4).alias("running_min_amount"),
        F.avg("amount").over(w4).alias("running_avg_amount"),
        F.sum("amount").over(w4).alias("running_sum_amount"),
        F.max("amount").over(w4).alias("running_max_amount"),
        F.first("amount", ignorenulls=True).over(w5).alias("first_known_amount"),
    )
)

matched_keys = (
    enriched
    .select("method", "id")
    .subtract(
        df_payments
        .filter(F.col("status").isin(["SUCCESS", "REFUNDED", "FAILED"]))
        .select("method", "id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.method") == F.col("k.method")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("running_min_amount").between(940, 1600))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        "method_filled",
        "island_id",
        "running_avg_amount",
        "running_sum_amount",
        "first_known_amount",
        F.coalesce("running_max_amount", F.lit(0)).alias("running_max_amount_filled"),
        (F.col("running_sum_amount") - F.col("island_id")).alias("running_sum_amount_minus_island_id"),
    )
)

enriched_payments = (
    filtered
    .select(
        "order_id",
        "method",
        "amount",
        "status",
        "method_filled",
        "island_id",
        "running_avg_amount",
        "running_sum_amount",
        "first_known_amount",
        "running_max_amount_filled",
        F.cume_dist().over(w6).alias("first_known_amount_cume_dist"),
        F.dense_rank().over(w7).alias("running_avg_amount_dense_rank"),
        F.round((F.col("running_sum_amount") / F.sum("running_sum_amount").over(w8)), 4).alias("running_sum_amount_share"),
        F.dense_rank().over(w9).alias("island_id_dense_rank"),
    )
)

result = (
    enriched_payments
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "method",
        "amount",
        "status",
        "island_id",
        "running_avg_amount",
        "running_sum_amount",
        "first_known_amount",
        "running_max_amount_filled",
        F.col("sum_shipment_cost"),
    )
)
