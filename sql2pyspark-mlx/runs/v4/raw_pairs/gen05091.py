from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy("event_time_period", "id")
w4 = Window.partitionBy("page").orderBy("event_time_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("session_id", "page").orderBy(F.col("new_group_flag").desc())
w6 = Window.orderBy("session_no")
w7 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w8 = Window.partitionBy("event_type").orderBy(F.col("new_group_flag").asc_nulls_last())
w9 = Window.partitionBy("page", "event_type")

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .withColumn("event_time_period", F.date_trunc("MONTH", "event_time"))
    .withColumn("resolved_page", F.coalesce("page", F.lit("N/A")))
)

gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

with_gaps = (
    islands
    .withColumn("new_group_flag", F.when(F.col("event_time_period") - F.lag("event_time_period").over(w3) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands_web_events = with_gaps.withColumn("session_id_2", F.sum("new_group_flag").over(w4))

remaining_keys = (
    islands_web_events
    .select("event_type")
    .subtract(
        df_web_events
        .filter(~F.col("page").isin(["/search", "/cart", "/checkout"]))
        .select("event_type")
    )
)

kept_rows = (
    islands_web_events
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .withColumn("new_group_flag_dense_rank", F.dense_rank().over(w5))
    .withColumn("session_no_rank", F.rank().over(w6))
    .withColumn("duration_sec_max_90d", F.max("duration_sec").over(w7))
)

scored = (
    metrics
    .withColumn("new_group_flag_rank", F.rank().over(w8))
    .withColumn("session_no_rank_2", F.rank().over(w6))
    .withColumn("new_group_flag_dense_rank_share", F.round((F.col("new_group_flag_dense_rank") / F.sum("new_group_flag_dense_rank").over(w9)), 4))
)

grouped = (
    scored
    .cube(
        "event_type",
        F.when(F.col("session_no") >= 8, "PREMIUM").otherwise("STANDARD").alias("session_no_tier"),
    )
    .agg(
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_rows"),
        F.min("event_time_period").alias("started_at"),
        F.sum(F.when(F.col("page") == "/home", F.col("duration_sec"))).alias("home_total_duration_sec"),
    )
)

result = (
    grouped
    .select("started_at")
    .orderBy(F.col("started_at").desc())
)
