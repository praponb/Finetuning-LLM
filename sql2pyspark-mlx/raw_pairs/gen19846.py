from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("page").orderBy("event_time", "id")
w3 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("burst_id")
w_grp = Window.partitionBy(F.to_date("prev_event_time"), "page").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("page").orderBy("burst_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-04-09 21:00:00").cast("timestamp"))
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        F.concat_ws("-", "event_type", "page").alias("event_type_page_key"),
        F.coalesce("page", F.lit("OTHER")).alias("page_filled"),
    )
)

with_windows = (
    filtered
    .filter(F.col("page") == "/search")
    .select(
        "id",
        "page",
        "event_time",
        F.first("page", ignorenulls=True).over(w).alias("first_known_page"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

with_prev = with_windows.withColumn("prev_event_time", F.lag("event_time").over(w2))

with_gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w3))

enriched = (
    islands
    .select(
        "id",
        "event_time",
        "first_known_page",
        "prev_event_time",
        F.rank().over(w4).alias("burst_id_rank"),
        F.sum("row_seq").over(w_grp).alias("cumulative_row_seq"),
        F.last("page", ignorenulls=True).over(w6).alias("last_known_page"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("burst_id_rank") >= 8, "HIGH").otherwise("LOW").alias("burst_id_rank_tier"),
        F.date_trunc("QUARTER", "event_time").alias("quarter_start"),
    )
    .agg(
        F.percentile_approx("burst_id_rank", [0.25, 0.5, 0.75]).alias("burst_id_rank_quartiles"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") > 1)
    .orderBy(F.col("burst_id_rank_tier").asc_nulls_last())
)
