from pyspark.sql import functions as F

prepared = (
    df_ticket_events
    .filter((F.col("actor") != "agent") & (F.col("event_type").isin("resolved", "replied", "created")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("quarter", F.col("event_at")).alias("event_at_quarter"),
    )
)

event_side = prepared.select(F.col("actor").alias("label"), F.col("event_type").alias("name"))

event_side_2 = (
    df_ticket_events
    .select(F.col("event_type").alias("label"), F.col("actor").alias("name"))
)

result = (
    event_side
    .union(event_side_2)
    .distinct()
    .orderBy(F.col("label").desc(), "name")
)
