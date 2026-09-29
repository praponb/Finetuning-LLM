from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at_running = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at = Window.orderBy("played_at", "id")

base_plays = (
    df_plays
    .filter((F.col("source") == "playlist") | (F.col("ms_played").between(108979, 299017)))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM-dd HH:00").alias("played_at_label"),
        F.concat(F.col("source"), F.lit("#"), F.col("ms_played").cast("string")).alias("source_ref"),
    )
)

metrics = (
    base_plays
    .select(
        "*",
        F.first("played_at", ignorenulls=True).over(w_all_by_played_at_running).alias("first_known_played_at"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_all_by_played_at))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.last("played_at", ignorenulls=True).over(w_all_by_played_at_running).alias("last_known_played_at"),
        F.round(F.cume_dist().over(w_all_by_played_at), 3).alias("played_at_cume_dist"),
        F.dense_rank().over(w_all_by_played_at).alias("played_at_dense_rank"),
    )
)

first_rows = (
    metrics
    .filter(F.col("played_at_dense_rank") <= 2)
    .drop("played_at_dense_rank")
)

result = (
    first_rows
    .select("id", "played_at_cume_dist", "minutes_since_prev_played_at")
    .unpivot(
        ids=["id"],
        values=["played_at_cume_dist", "minutes_since_prev_played_at"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .orderBy("id", "metric")
)
