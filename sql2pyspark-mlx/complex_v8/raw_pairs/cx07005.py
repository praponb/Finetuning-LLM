from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at_day = Window.partitionBy("ticket_id").orderBy("event_at_day", "id")
w_all_by_event_at_day_ties = Window.orderBy("event_at_day")
w_event_id_event_actor = Window.partitionBy("event_id", "event_actor")
w_ticket_id_actor_by_event_eve = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_event_at").desc())

filtered = (
    df_ticket_events
    .filter((F.col("actor") == "customer") & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("day", F.col("event_at")).alias("event_at_day"),
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.row_number().over(w_ticket_id_by_event_at_day).alias("rn"),
        F.dense_rank().over(w_all_by_event_at_day_ties).alias("event_at_day_dense_rank"),
    )
)

with_children_events = (
    metrics.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type") != "reopened"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_at_day",
        "s.event_at_date",
        "s.rn",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

sequenced = (
    with_children_events
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "event_at_day",
        "event_id",
        "event_actor",
        "event_event_type",
        F.round(F.col("rn") / F.nullif(F.sum("rn").over(w_event_id_event_actor), F.lit(0)), 4).alias("rn_share_of_event_id_event_actor"),
        F.dense_rank().over(w_ticket_id_actor_by_event_eve).alias("event_event_at_dense_rank"),
    )
)

with_parent_event = (
    sequenced.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "inner")
    .filter(F.col("t.actor").isin("agent", "customer"))
    .select(
        "s.id",
        "s.event_type",
        "s.actor",
        "s.event_at_day",
        "s.event_id",
        "s.event_event_type",
        "s.rn_share_of_event_id_event_actor",
        "s.event_event_at_dense_rank",
        F.col("t.event_type").alias("event_event_type_2"),
        F.col("t.actor").alias("event_actor_2"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

leaders = with_parent_event.filter(F.col("event_event_at_dense_rank") == 1)

qualified = (
    leaders.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.id") == F.col("s.event_id")) & (F.col("t.actor").isNotNull()) & (F.col("t.event_type") == "reopened"), "left_semi")
)

result = (
    qualified
    .select(
        "id",
        "event_type",
        "actor",
        "event_id",
        "event_event_at_dense_rank",
        "event_actor_2",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.concat(F.col("actor"), F.lit("#"), F.col("event_id").cast("string")).alias("actor_ref"),
    )
    .orderBy(F.col("event_id").desc())
)
