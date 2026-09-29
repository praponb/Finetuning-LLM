from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_by_ms_played = Window.partitionBy("listener_id").orderBy("ms_played", "id")
w_source_by_played_at = Window.partitionBy("source").orderBy("played_at", "id")
w_all_by_played_at = Window.orderBy(F.col("played_at").asc_nulls_last(), "id")
w_source = Window.partitionBy("source")

base = (
    df_plays
    .filter((F.col("ms_played").between(92277, 351217)) & (F.col("track_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        F.lower(F.trim(F.col("source"))).alias("source_clean"),
    )
)

ranked = (
    base
    .select(
        "*",
        (F.col("ms_played") - F.lag("ms_played").over(w_listener_id_by_ms_played)).alias("ms_played_change"),
        F.lag("ms_played", 1, 0).over(w_source_by_played_at).alias("prev_ms_played"),
        F.lead("ms_played", 2).over(w_all_by_played_at).alias("next2_ms_played"),
        F.ntile(5).over(w_all_by_played_at).alias("played_at_quintile"),
        F.lag("played_at").over(w_listener_id_by_ms_played).alias("prev_played_at"),
        F.size(F.collect_set("listener_id").over(w_source)).alias("distinct_listener_id_in_source"),
        F.ntile(3).over(w_listener_id_by_ms_played).alias("ms_played_tercile"),
    )
)

labeled = (
    ranked
    .filter(F.col("next2_ms_played") <= 299017)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "source",
        "source_clean",
        "ms_played_change",
        "prev_ms_played",
        "next2_ms_played",
        "played_at_quintile",
        "prev_played_at",
        "distinct_listener_id_in_source",
        "ms_played_tercile",
        F.least(F.col("ms_played_change"), F.col("ms_played")).alias("least_ms_played_change_ms_played"),
    )
)

result = (
    labeled
    .select(
        "id",
        "listener_id",
        "played_at",
        "source",
        "prev_ms_played",
        "next2_ms_played",
        "played_at_quintile",
        "prev_played_at",
        "distinct_listener_id_in_source",
        "ms_played_tercile",
        "least_ms_played_change_ms_played",
    )
    .orderBy(
        F.col("prev_ms_played").desc(),
        F.col("prev_played_at").asc_nulls_last(),
    )
)
