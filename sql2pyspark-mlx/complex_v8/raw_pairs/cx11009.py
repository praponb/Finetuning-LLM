from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_3d_played_at_year = Window.partitionBy("prior_rows_3d", F.year("played_at")).orderBy("prior_rows_3d", "id").rowsBetween(-6, -1)
w_prior_rows_3d_played_at_year_2 = Window.partitionBy("prior_rows_3d", F.year("played_at")).orderBy("prior_rows_3d", "id")
w_prior_rows_3d_played_at_year_3 = Window.partitionBy("prior_rows_3d", F.year("played_at")).orderBy("prior_rows_3d", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_played_at_month = Window.partitionBy("listener_id", F.month("played_at"))

matched = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.ms_played").alias("prior_ms_played_3d"),
    )
)

ranked = (
    matched
    .select(
        "id",
        "listener_id",
        "prior_rows_3d",
        "prior_ms_played_3d",
        F.max("prior_ms_played_3d").over(w_prior_rows_3d_played_at_year).alias("prior_ms_played_3d_max_6_prior"),
        F.dense_rank().over(w_prior_rows_3d_played_at_year_2).alias("prior_rows_3d_dense_rank"),
        F.first("played_at").over(w_prior_rows_3d_played_at_year_3).alias("first_played_at"),
        F.count(F.lit(1)).over(w_listener_id_played_at_month).alias("rows_in_listener_id_played_at_month"),
    )
)

result = (
    ranked
    .select(
        "id",
        "prior_rows_3d",
        "prior_ms_played_3d",
        "prior_ms_played_3d_max_6_prior",
        "prior_rows_3d_dense_rank",
        "rows_in_listener_id_played_at_month",
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
