from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("shipped_date_period", "id")
w2 = Window.partitionBy("order_id").orderBy("shipped_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id")
w4 = Window.partitionBy("order_id").orderBy("seq_no")
w5 = Window.partitionBy(F.month("delivered_date"), "order_id").orderBy(F.col("prev_shipped_date_period_2").cast("bigint")).rangeBetween(-604800, Window.currentRow)

valid = (
    df_shipments
    .filter(F.col("order_id").isin([33, 34, 40]) & (F.col("shipped_date") > F.lit("2023-02-20").cast("date")))
    .select(
        "id",
        "order_id",
        "delivered_date",
        F.date_trunc("MONTH", "shipped_date").alias("shipped_date_period"),
        F.substring("carrier", 1, 4).alias("carrier_prefix"),
    )
)

with_windows = (
    valid
    .filter(F.col("order_id").isNotNull())
    .withColumn("seq_no", F.row_number().over(w))
)

prev_events = (
    with_windows
    .withColumn("prev_shipped_date_period", F.lag("shipped_date_period").over(w))
)

flagged = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_shipped_date_period").isNull() | (F.col("shipped_date_period") > F.col("prev_shipped_date_period") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_burst").over(w2))

prev_events_2 = (
    sessionized
    .withColumn("prev_shipped_date_period_2", F.lag("shipped_date_period").over(w))
)

with_gaps = (
    prev_events_2
    .withColumn("session_start_flag", F.when(F.col("prev_shipped_date_period_2").isNull() | (F.col("shipped_date_period") > F.col("prev_shipped_date_period_2") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized_shipments = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

kept_keys = (
    sessionized_shipments
    .select("order_id", "id")
    .intersect(
        df_shipments
        .filter(F.col("shipped_date") <= "2024-01-01")
        .select("order_id", "id")
    )
)

verified = (
    sessionized_shipments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .withColumn("order_id_max_seq_no", F.max("seq_no").over(w3))
    .withColumn("seq_no_dense_rank", F.dense_rank().over(w4))
    .withColumn("session_no_sum_7d", F.sum("session_no").over(w5))
)

with_dims = (
    scored
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.discount"), F.col("b.amount"))
)

result = (
    with_dims
    .groupBy(
        F.date_trunc("QUARTER", "prev_shipped_date_period").alias("quarter_start"),
        F.when(F.col("session_start_flag") >= 1, "LARGE").otherwise("SMALL").alias("session_start_flag_tier"),
        "island_id",
    )
    .agg(
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("prev_shipped_date_period").alias("last_seen"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("distinct_ids") < 1000)
    .orderBy(F.col("quarter_start").desc_nulls_first())
)
