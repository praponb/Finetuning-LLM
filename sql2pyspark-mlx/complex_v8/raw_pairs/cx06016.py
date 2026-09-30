from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), F.col("id").desc())
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id")
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id")

filtered = (
    df_ticket_events
    .filter(F.col("event_type") == "resolved")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(": ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
)

latest = (
    filtered
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

sequenced = (
    latest
    .select(
        "id",
        "event_at",
        "actor",
        "actor_event_type_label",
        F.round(F.percent_rank().over(w_event_type_by_event_at), 3).alias("event_at_percent_rank"),
        F.lead("event_at").over(w_event_type_by_event_at).alias("next_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_id_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.lead("event_at", 2).over(w_ticket_id_by_event_at).alias("next2_event_at"),
    )
)

result = (
    sequenced
    .groupBy("next_event_at")
    .pivot("actor", ["customer", "agent"])
    .agg(F.sum("minutes_since_prev_event_at").alias("total"), F.count("minutes_since_prev_event_at").alias("n"))
    .orderBy("next_event_at")
)
