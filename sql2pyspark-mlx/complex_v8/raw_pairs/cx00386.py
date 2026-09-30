from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("agent_id").orderBy(F.col("requester_id").desc(), F.col("id").desc())

scoped = (
    df_tickets
    .filter((F.col("priority") == "P3") & (~F.col("channel").isin("phone", "web")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
    )
)

latest = (
    scoped
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

annotated = (
    latest
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 60, 1).alias("minutes_opened_at_to_closed_at"),
        F.coalesce(F.col("days_since_closed_at"), F.lit(0)).alias("days_since_closed_at_filled"),
    )
)

with_activity = (
    annotated.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_at") >= F.lit("2025-01-25 07:00:00").cast("timestamp")) & (F.col("t.actor") == "system"), "left_semi")
)

result = (
    with_activity
    .select("closed_at", "days_since_closed_at_filled")
    .distinct()
)
