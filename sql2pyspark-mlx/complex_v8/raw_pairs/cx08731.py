from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy("event_at", "id")
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_actor_ticket_id_by_event_at_2 = Window.partitionBy("actor", "ticket_id").orderBy("event_at")
w_latest = Window.partitionBy("actor").orderBy(F.col("rn").desc(), F.col("id").desc())

prepared_events = (
    df_ticket_events
    .filter(F.col("event_type").isin("created", "escalated"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_trends = (
    prepared_events
    .select(
        "*",
        F.ntile(4).over(w_actor_ticket_id_by_event_at).alias("event_at_quartile"),
        F.lag("event_at").over(w_actor_ticket_id_by_event_at).alias("prev_event_at"),
        F.row_number().over(w_ticket_id_by_event_at).alias("event_at_seq"),
        F.row_number().over(w_actor_ticket_id_by_event_at).alias("rn"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_id_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.round(F.cume_dist().over(w_actor_ticket_id_by_event_at_2), 3).alias("event_at_cume_dist"),
    )
)

deduped = (
    with_trends
    .withColumn("rn_2", F.row_number().over(w_latest))
    .filter(F.col("rn_2") == 1)
    .drop("rn_2")
)

result = (
    deduped
    .groupBy("event_at_quartile")
    .pivot("actor", ["agent", "system", "customer"])
    .agg(F.sum("minutes_since_prev_event_at"))
)
