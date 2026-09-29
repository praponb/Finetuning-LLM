from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_prior_rows_3d = Window.partitionBy("listener_id", "prior_rows_3d")

in_window = (
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

annotated = in_window.withColumn("prior_rows_3d_filled", F.coalesce(F.col("prior_rows_3d"), F.lit(0)))

metrics = (
    annotated
    .select(
        "*",
        F.round(F.col("prior_ms_played_3d") / F.nullif(F.sum("prior_ms_played_3d").over(w_listener_id_prior_rows_3d), F.lit(0)), 4).alias("prior_ms_played_3d_share_of_listener_id_prior_ro"),
        F.count(F.lit(1)).over(w_listener_id_prior_rows_3d).alias("rows_in_listener_id_prior_rows_3d"),
    )
)

result = (
    metrics
    .select("id", "prior_rows_3d", "prior_ms_played_3d")
    .unpivot(
        ids=["id"],
        values=["prior_rows_3d", "prior_ms_played_3d"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "metric_name")
)
