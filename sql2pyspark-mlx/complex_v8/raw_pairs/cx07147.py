from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("agent_id").orderBy(F.col("prior_csat_3d").desc(), F.col("id").desc())

covered = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.csat").alias("prior_csat_3d"),
    )
)

latest_per_key = (
    covered
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

without_matches = (
    latest_per_key.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp")), "left_semi")
    .filter(F.col("prior_csat_3d") < 20)
)

labeled = (
    without_matches
    .filter(F.col("prior_csat_3d") < 22)
    .select(
        "id",
        "agent_id",
        "prior_rows_3d",
        F.round(F.col("prior_csat_3d") / F.nullif(F.col("prior_rows_3d"), F.lit(0)), 3).alias("prior_csat_3d_per_prior_rows_3d"),
        F.abs(F.col("prior_csat_3d") - 15).alias("prior_csat_3d_distance"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
)

result = (
    labeled
    .filter(F.col("prior_csat_3d_distance") < 14)
    .select(
        "id",
        "agent_id",
        "prior_rows_3d",
        "prior_csat_3d_per_prior_rows_3d",
        "prior_csat_3d_distance",
        "days_since_opened_at",
        F.round(F.col("prior_csat_3d_distance") / F.nullif(F.col("prior_rows_3d"), F.lit(0)), 3).alias("prior_csat_3d_distance_per_prior_rows_3d"),
        F.least(F.col("days_since_opened_at"), F.col("prior_csat_3d_distance")).alias("least_days_since_opened_at_prior_csat_3d_distanc"),
    )
    .orderBy(F.col("id").desc())
)
