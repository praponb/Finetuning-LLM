from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy(F.col("event_time_day").desc_nulls_last(), "id")
w_grp = Window.orderBy("event_time", "id")

base = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("page").eqNullSafe(F.lit("/home"))))
    .select(
        "id",
        "page",
        "event_time",
        F.concat_ws("/", "event_type", "page").alias("event_type_page_key"),
    )
)

valid = (
    base
    .filter(F.col("page").isNotNull())
    .select(
        "*",
        F.col("event_time").cast("date").alias("event_time_day"),
        F.coalesce("page", F.lit("NONE")).alias("page_clean"),
    )
)

prev_events = valid.withColumn("prev_event_time", F.lag("event_time").over(w))

with_gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

deduped = (
    sessions_web_events
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

base_web_events = (
    deduped
    .filter((F.col("page").eqNullSafe(F.lit("/cart"))) & (F.col("event_time") > F.to_timestamp(F.lit("2023-06-19 15:00:00"))))
    .withColumn("island_id_plus_rank_in_group", (F.col("island_id") + F.col("rank_in_group")))
)

valid_web_events = (
    base_web_events
    .filter((F.col("event_time") > F.lit("2023-03-10").cast("date")) & (F.col("island_id_plus_rank_in_group") <= 71))
    .select(
        "id",
        "event_time",
        "event_time_day",
        "prev_event_time",
        "session_start_flag",
        "island_id",
        "rank_in_group",
        "island_id_plus_rank_in_group",
        F.coalesce("session_start_flag", F.lit(0)).alias("session_start_flag_filled"),
        (F.col("session_start_flag") + F.col("island_id")).alias("session_start_flag_plus_island_id"),
        F.coalesce("page", F.lit("N/A")).alias("page_clean_2"),
    )
)

result = (
    valid_web_events
    .select(
        "id",
        "event_time",
        "event_time_day",
        "prev_event_time",
        "session_start_flag",
        "island_id",
        "rank_in_group",
        "island_id_plus_rank_in_group",
        "session_start_flag_filled",
        "session_start_flag_plus_island_id",
        "page_clean_2",
        F.lag("session_start_flag_plus_island_id").over(w_grp).alias("prev_session_start_flag_plus_island_id"),
        F.lead("session_start_flag_plus_island_id").over(w_grp).alias("next_session_start_flag_plus_island_id"),
        F.lag("island_id").over(w_grp).alias("prev_island_id"),
        F.lead("island_id").over(w_grp).alias("next_island_id"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)
