from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_len_ticket_id_by_actor = Window.partitionBy("actor_len", "ticket_id").orderBy("actor_len")
w_actor_len_ticket_id_by_actor_2 = Window.partitionBy("actor_len", "ticket_id").orderBy("actor_len", "id")

recent = (
    df_ticket_events
    .filter((F.col("event_type").isNotNull()) & (F.col("ticket_id") >= 59))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

tagged = (
    recent
    .select(
        "id",
        "ticket_id",
        "days_since_event_at",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
    )
)

metrics = (
    tagged
    .select(
        "id",
        "ticket_id",
        "days_since_event_at",
        "event_type_ref",
        F.rank().over(w_actor_len_ticket_id_by_actor).alias("actor_len_rank"),
        (F.col("days_since_event_at") - F.lag("days_since_event_at").over(w_actor_len_ticket_id_by_actor_2)).alias("days_since_event_at_change"),
    )
)

result = (
    metrics
    .rollup("actor_len_rank", "ticket_id")
    .agg(
        F.grouping("actor_len_rank").alias("is_total_actor_len_rank"),
        F.count("*").alias("cnt"),
        F.round(F.stddev("days_since_event_at"), 2).alias("stddev_days_since_event_at"),
    )
    .orderBy(
        F.col("actor_len_rank").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
    )
)
