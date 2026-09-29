from pyspark.sql import functions as F

scoped = (
    df_tickets
    .filter(F.to_date(F.col("closed_at")) < F.lit("2025-02-08").cast("date"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "tags",
        (F.col("opened_at") + F.expr("INTERVAL 2 HOURS")).alias("opened_at_plus_2h"),
    )
)

ticket_side = scoped.select("channel")

ticket_rows = (
    df_tickets
    .filter(F.col("priority") == "P1")
    .select(F.col("priority").alias("channel"))
)

result = (
    ticket_side
    .subtract(ticket_rows)
    .orderBy("channel")
)
