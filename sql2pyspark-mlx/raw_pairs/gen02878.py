from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("page").orderBy(F.col("is_new_burst").desc(), "id")

base = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("event_type") != "purchase"))
    .withColumn("event_type_group", F.when(F.col("event_type").isin(["add_to_cart", "search"]), "Group A").otherwise("Other"))
)

flagged = (
    base
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w2))

with_windows = (
    islands
    .select(
        "*",
        F.count("is_new_burst").over(w_hist).alias("last4_count_is_new_burst"),
        F.max("is_new_burst").over(w_hist).alias("last4_max_is_new_burst"),
        F.sum("is_new_burst").over(w_hist).alias("last4_sum_is_new_burst"),
    )
)

deduped = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

remaining_keys = (
    deduped
    .select("id", "event_type")
    .intersect(
        df_web_events
        .filter(F.col("page").isin(["/cart", "/checkout", "/home"]))
        .select("id", "event_type")
    )
)

verified = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter((F.col("event_time") > F.to_date(F.lit("2023-09-06"))) & (~F.col("page").eqNullSafe(F.lit("/home"))))
    .select(
        "session_id",
        "event_time",
        "duration_sec",
        "event_type_group",
        "burst_id",
        "last4_count_is_new_burst",
        "last4_max_is_new_burst",
        "last4_sum_is_new_burst",
        "dedup_rank",
        F.floor("dedup_rank").alias("dedup_rank_whole"),
    )
)

result = (
    cleaned
    .groupBy("burst_id")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("session_id").alias("distinct_session_ids"),
        F.count("*").alias("row_count"),
        F.max("event_time").alias("last_seen"),
        F.percentile_approx("last4_count_is_new_burst", [0.25, 0.5, 0.75]).alias("last4_count_is_new_burst_quartiles"),
        F.countDistinct("session_id").alias("distinct_session_ids_2"),
    )
    .orderBy("burst_id")
)
