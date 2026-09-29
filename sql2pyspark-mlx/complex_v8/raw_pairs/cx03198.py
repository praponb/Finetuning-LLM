from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_to_date = Window.partitionBy(F.to_date("played_at"))

covered = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.ms_played").alias("prior_ms_played_1d"),
    )
)

sequenced = (
    covered
    .select(
        "id",
        "played_at",
        "prior_rows_1d",
        "prior_ms_played_1d",
        F.count(F.lit(1)).over(w_played_at_to_date).alias("rows_in_played_at_to_date"),
        F.round(F.col("prior_rows_1d") / F.nullif(F.sum("prior_rows_1d").over(w_played_at_to_date), F.lit(0)), 4).alias("prior_rows_1d_share_of_played_at_to_date"),
        F.round(F.avg("prior_ms_played_1d").over(w_played_at_to_date), 2).alias("played_at_to_date_avg_prior_ms_played_1d"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "prior_rows_1d",
        "rows_in_played_at_to_date",
        "prior_rows_1d_share_of_played_at_to_date",
        "played_at_to_date_avg_prior_ms_played_1d",
        F.abs(F.col("prior_ms_played_1d") - 1100000).alias("prior_ms_played_1d_distance"),
    )
    .orderBy("prior_ms_played_1d_distance", "id")
    .limit(5)
)
