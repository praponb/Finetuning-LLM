from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_event_at_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at_moving_1 = Window.orderBy("event_at", "id").rowsBetween(-1, Window.currentRow)
w_actor_by_event_at_full = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_events = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) | (F.month(F.col("event_at")).isin(1, 3)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws("|", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

metrics = (
    prepared_events
    .select(
        "*",
        F.max("actor_len").over(w_actor_by_event_at_running).alias("max_actor_len_so_far"),
        F.sum("actor_len").over(w_all_by_event_at_moving_1).alias("actor_len_sum_2"),
        F.first("event_at").over(w_actor_by_event_at_full).alias("first_event_at"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("event_type").isNotNull(), 1)).alias("event_event_type_count"),
    )
)

summarized_rows = (
    metrics.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "actor_len_sum_2",
        "event_event_type_count",
        "max_actor_len_so_far",
        "event_count",
    )
    .unpivot(
        ids=["id"],
        values=["actor_len_sum_2", "event_event_type_count", "max_actor_len_so_far", "event_count"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
