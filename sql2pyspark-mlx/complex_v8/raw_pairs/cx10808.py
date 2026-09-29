from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_last_event_at_year = Window.partitionBy(F.year("last_event_at"))
w_prior_rows_12h_by_opened_at = Window.partitionBy("prior_rows_12h").orderBy("opened_at")
w_prior_rows_12h_by_opened_at_2 = Window.partitionBy("prior_rows_12h").orderBy("opened_at", "id")

joined_ranges = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.csat").alias("prior_csat_12h"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

summarized_rows = (
    joined_ranges.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select("s.*", "t.last_event_at")
)

enriched = (
    summarized_rows
    .filter(F.hour(F.col("last_event_at")).between(7, 11))
    .select(
        "*",
        (F.col("prior_rows_12h") + F.col("prior_csat_12h")).alias("prior_rows_12h_plus_prior_csat_12h"),
        F.year(F.col("opened_at")).alias("opened_at_year"),
    )
)

with_trends = (
    enriched
    .select(
        "*",
        F.round(F.avg("prior_rows_12h").over(w_last_event_at_year), 2).alias("last_event_at_year_avg_prior_rows_12h"),
        F.round(F.cume_dist().over(w_prior_rows_12h_by_opened_at), 3).alias("opened_at_cume_dist"),
        F.round(F.percent_rank().over(w_prior_rows_12h_by_opened_at_2), 3).alias("opened_at_percent_rank"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "opened_at_year",
        "prior_csat_12h",
        "prior_rows_12h_plus_prior_csat_12h",
        "prior_rows_12h",
    )
    .unpivot(
        ids=["id"],
        values=["opened_at_year", "prior_csat_12h", "prior_rows_12h_plus_prior_csat_12h", "prior_rows_12h"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
