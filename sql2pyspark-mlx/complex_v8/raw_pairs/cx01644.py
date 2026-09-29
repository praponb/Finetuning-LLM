from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_at").desc(), F.col("id").desc())

prepared_events = (
    df_ticket_events
    .filter(F.col("actor").isin("agent", "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref_2"),
    )
)

scored = (
    prepared_events
    .filter(F.col("event_type") == "escalated")
    .select(
        "*",
        F.date_trunc("quarter", F.col("event_at")).alias("event_at_quarter"),
        (F.col("event_at") + F.expr("INTERVAL 2 HOURS")).alias("event_at_plus_2h"),
    )
)

deduped = (
    scored
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .select(
        "id",
        "event_at",
        "actor",
        "actor_slug",
        "event_type_ref_2",
        "event_at_quarter",
        "event_at_plus_2h",
        F.date_trunc("week", F.col("event_at_plus_2h")).alias("event_at_plus_2h_week"),
    )
    .orderBy(F.col("event_type_ref_2").asc(), "actor_slug")
)
