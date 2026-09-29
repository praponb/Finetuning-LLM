from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_prior_ms_played_7d = Window.orderBy(F.col("prior_ms_played_7d").desc(), "id")
w_all_by_prior_ms_played_7d_ti = Window.orderBy(F.col("prior_ms_played_7d").desc())

joined_ranges = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.ms_played").alias("prior_ms_played_7d"),
    )
)

sequenced = (
    joined_ranges
    .select(
        "id",
        "listener_id",
        "played_at",
        "prior_rows_7d",
        F.lead("prior_ms_played_7d").over(w_all_by_prior_ms_played_7d).alias("next_prior_ms_played_7d"),
        F.ntile(3).over(w_all_by_prior_ms_played_7d).alias("prior_ms_played_7d_tercile"),
        F.round(F.cume_dist().over(w_all_by_prior_ms_played_7d_ti), 3).alias("prior_ms_played_7d_cume_dist"),
        F.lead("played_at", 2).over(w_all_by_prior_ms_played_7d).alias("next2_played_at"),
    )
)

labeled = (
    sequenced
    .select(
        "id",
        "prior_rows_7d",
        "next_prior_ms_played_7d",
        "prior_ms_played_7d_tercile",
        "prior_ms_played_7d_cume_dist",
        "next2_played_at",
        F.coalesce(F.col("prior_rows_7d"), F.lit(0)).alias("prior_rows_7d_filled"),
    )
)

result = (
    labeled
    .rollup("prior_rows_7d_filled", "prior_ms_played_7d_tercile", "prior_rows_7d")
    .agg(
        F.min("prior_rows_7d").alias("min_prior_rows_7d"),
        F.count("*").alias("cnt"),
        F.sum("prior_rows_7d_filled").alias("total_prior_rows_7d_filled"),
    )
    .orderBy(
        F.col("prior_rows_7d_filled").asc_nulls_last(),
        F.col("prior_ms_played_7d_tercile").asc_nulls_last(),
        F.col("prior_rows_7d").asc_nulls_last(),
    )
)
