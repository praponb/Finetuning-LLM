from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_event_at_ties = Window.partitionBy("actor").orderBy("event_at")

cleaned_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

metrics = cleaned_events.select("*", F.rank().over(w_actor_by_event_at_ties).alias("event_at_rank"))

result = (
    metrics
    .select(
        "id",
        "event_type",
        F.substring(F.trim(F.col("actor")), 1, 4).alias("actor_prefix"),
        F.hour(F.col("event_at")).alias("event_at_hour"),
    )
    .orderBy("actor_prefix")
)
