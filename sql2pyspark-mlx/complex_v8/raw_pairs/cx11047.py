from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_by_played_at_plu = Window.partitionBy("listener_id").orderBy("played_at_plus_1d", "id")
w_listener_id_by_played_at_plu_2 = Window.partitionBy("listener_id").orderBy("played_at_plus_1d", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_by_played_at_plu_3 = Window.partitionBy("listener_id").orderBy("played_at_plus_1d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source = Window.partitionBy("source")
w_all_by_played_at_plus_1d = Window.orderBy(F.col("played_at_plus_1d").desc(), "id")
w_is_skipped_label_by_played_a = Window.partitionBy("is_skipped_label").orderBy("played_at_plus_1d", "id")

base_plays = (
    df_plays
    .filter((F.col("source") == "playlist") & (F.col("device") == "desktop"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("source").isin("playlist", "album"), "group_a").when(F.col("source") == "search", "group_b").otherwise("other").alias("source_group"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
)

annotated = (
    base_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        "is_skipped_label",
        F.when(F.col("ms_played") >= 299017, "A").when(F.col("ms_played") >= 112786, "B").otherwise("C").alias("ms_played_tier"),
        (F.col("played_at") + F.expr("INTERVAL 1 DAYS")).alias("played_at_plus_1d"),
        F.to_date(F.col("played_at")).alias("played_at_date"),
    )
)

with_trends = (
    annotated
    .select(
        "*",
        F.datediff(F.col("played_at_date"), F.lag("played_at_date").over(w_listener_id_by_played_at_plu)).alias("days_since_prev_played_at_date"),
        F.rank().over(w_listener_id_by_played_at_plu).alias("played_at_plus_1d_rank"),
        F.first("played_at_date").over(w_listener_id_by_played_at_plu_2).alias("first_played_at_date"),
        F.first("played_at_date", ignorenulls=True).over(w_listener_id_by_played_at_plu_3).alias("first_known_played_at_date"),
    )
)

metrics = (
    with_trends
    .select(
        "*",
        F.size(F.collect_set("listener_id").over(w_source)).alias("distinct_listener_id_in_source"),
        F.row_number().over(w_all_by_played_at_plus_1d).alias("rn"),
        F.row_number().over(w_is_skipped_label_by_played_a).alias("played_at_plus_1d_seq"),
    )
)

result = (
    metrics
    .filter(F.year(F.col("first_played_at_date")) == 2025)
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        "source",
        "is_skipped_label",
        "ms_played_tier",
        "played_at_date",
        "days_since_prev_played_at_date",
        "played_at_plus_1d_rank",
        "first_played_at_date",
        "first_known_played_at_date",
        "distinct_listener_id_in_source",
        "rn",
        "played_at_plus_1d_seq",
        F.date_trunc("month", F.col("played_at_date")).alias("played_at_date_month"),
    )
    .orderBy(F.col("played_at_plus_1d_rank").asc(), F.col("source").desc())
)
