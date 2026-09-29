from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("prior_ms_played_12h_tier").orderBy("played_at", "id")

matched = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.ms_played").alias("prior_ms_played_12h"),
    )
)

with_activity = (
    matched.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.ms_played") <= 302279) & (F.col("p.source") != "radio"), "left_anti")
)

derived = (
    with_activity
    .select(
        "id",
        "played_at",
        "prior_rows_12h",
        F.when(F.col("prior_ms_played_12h") >= 1600000, "HIGH").when(F.col("prior_ms_played_12h") >= 700000, "MEDIUM").otherwise("LOW").alias("prior_ms_played_12h_tier"),
        F.year(F.col("played_at")).alias("played_at_year"),
        F.date_format(F.col("played_at"), "yyyy-MM").alias("played_at_label"),
    )
)

unique_rows = (
    derived
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .select(
        "id",
        F.col("played_at_year").alias("played_at_year_v"),
        F.col("prior_rows_12h").alias("prior_rows_12h_v"),
    )
    .unpivot(
        ids=["id"],
        values=["played_at_year_v", "prior_rows_12h_v"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "metric_name")
)
