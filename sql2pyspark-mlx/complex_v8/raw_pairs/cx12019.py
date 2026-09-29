from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), F.col("id").desc())
w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_at_week").desc(), "id")
w_actor_ticket_id_by_event_at_2 = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_at_week").desc())

recent = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
    )
)

deduped = (
    recent
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

sequenced = (
    deduped
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lag("event_at_week").over(w_actor_ticket_id_by_event_at).alias("prev_event_at_week"),
        F.round(F.percent_rank().over(w_actor_ticket_id_by_event_at), 3).alias("event_at_week_percent_rank"),
        F.dense_rank().over(w_actor_ticket_id_by_event_at).alias("event_at_week_dense_rank"),
        F.rank().over(w_actor_ticket_id_by_event_at).alias("event_at_week_rank"),
        F.rank().over(w_actor_ticket_id_by_event_at_2).alias("event_at_week_rank_3"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "ticket_id",
        "event_type",
        "event_at_week_rank",
        F.round((F.unix_timestamp(F.col("prev_event_at_week")) - F.unix_timestamp(F.col("event_at"))) / 3600, 1).alias("hours_event_at_to_prev_event_at_week"),
        F.date_format(F.col("prev_event_at_week"), "yyyy-MM").alias("prev_event_at_week_label"),
    )
    .orderBy(F.col("event_type").desc(), F.col("event_at_week_rank").asc(), "id")
    .limit(10)
)
