from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("event_type", "ticket_id").orderBy("event_at_plus_30m", "id")
w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc())
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id")
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_events = (
    df_ticket_events
    .filter((F.col("actor") == "system") & (F.col("event_type").isin("replied", "escalated", "created")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        (F.col("event_at") + F.expr("INTERVAL 30 MINUTES")).alias("event_at_plus_30m"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
    )
)

latest_per_key = (
    prepared_events
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

ranked = (
    latest_per_key
    .select(
        "*",
        F.rank().over(w_ticket_id_by_event_at_ties).alias("event_at_rank"),
        F.row_number().over(w_ticket_id_by_event_at).alias("event_at_seq"),
        F.nth_value("event_at", 2).over(w_ticket_id_by_event_at_full).alias("second_event_at"),
    )
)

result = (
    ranked
    .select(
        "id",
        "event_at",
        "event_type",
        "event_at_plus_30m",
        "event_at_seq",
        "second_event_at",
    )
)
