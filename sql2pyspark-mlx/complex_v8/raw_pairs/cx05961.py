from pyspark.sql import functions as F

valid = (
    df_ticket_events
    .filter((F.col("ticket_id") >= 49) | (~F.col("event_type").isin("replied", "reopened")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

labeled = (
    valid
    .select(
        "id",
        "ticket_id",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
        F.date_trunc("hour", F.col("event_at")).alias("event_at_hour"),
        F.substring(F.trim(F.col("event_type")), 1, 3).alias("event_type_prefix"),
    )
)

result = (
    labeled
    .filter(F.col("event_type_prefix").isNotNull())
    .select(
        "id",
        "ticket_id",
        "actor",
        "event_at_label",
        F.length(F.trim(F.col("event_type_prefix"))).alias("event_type_prefix_len"),
    )
    .orderBy("event_type_prefix_len")
)
