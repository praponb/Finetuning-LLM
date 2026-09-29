from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc_nulls_first(), "id")
w_event_type_by_ticket_opened = Window.partitionBy("event_type").orderBy("ticket_opened_at", "id")
w_event_type_by_ticket_opened_2 = Window.partitionBy("event_type").orderBy("ticket_opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ticket_opened_at_ties = Window.orderBy("ticket_opened_at")
w_event_type_by_ticket_opened_3 = Window.partitionBy("event_type").orderBy("ticket_opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy(F.col("event_at").desc())
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_ticket_opened_at = Window.orderBy("ticket_opened_at", "id")
w_event_type_by_ticket_opened_4 = Window.partitionBy("event_type").orderBy("ticket_opened_at")
w_event_type = Window.partitionBy("event_type")
w_ticket_agent_id_by_total_eve = Window.partitionBy("ticket_agent_id").orderBy("total_event_at_percent_rank")
w_ticket_agent_id_by_total_eve_2 = Window.partitionBy("ticket_agent_id").orderBy("total_event_at_percent_rank", "actor", "event_type", "ticket_agent_id")

prepared = (
    df_ticket_events
    .filter((F.month(F.col("event_at")).isin(1, 3)) & (F.col("ticket_id") >= 17))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

with_parent = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.actor_slug",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

ranked = (
    with_parent
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_slug",
        "ticket_opened_at",
        "ticket_agent_id",
        F.lead("ticket_opened_at").over(w_ticket_id_by_event_at).alias("next_ticket_opened_at"),
        F.round(F.cume_dist().over(w_event_type_by_ticket_opened), 3).alias("ticket_opened_at_cume_dist"),
        F.last("ticket_opened_at", ignorenulls=True).over(w_event_type_by_ticket_opened_2).alias("last_known_ticket_opened_at"),
        F.round(F.percent_rank().over(w_all_by_ticket_opened_at_ties), 3).alias("ticket_opened_at_percent_rank"),
        F.nth_value("ticket_opened_at", 2).over(w_event_type_by_ticket_opened_3).alias("second_ticket_opened_at"),
        F.round(F.percent_rank().over(w_event_type_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.last("event_at").over(w_ticket_id_by_event_at_full).alias("last_event_at"),
        F.lag("ticket_opened_at", 3).over(w_all_by_ticket_opened_at).alias("prev3_ticket_opened_at"),
        F.dense_rank().over(w_event_type_by_ticket_opened_4).alias("ticket_opened_at_dense_rank"),
    )
)

summary = (
    ranked
    .groupBy("actor", "event_type", "ticket_agent_id")
    .agg(
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.max("last_event_at").alias("last_last_event_at"),
        F.min("prev3_ticket_opened_at").alias("first_prev3_ticket_opened_at"),
        F.countDistinct("ticket_agent_id").alias("distinct_ticket_agent_ids"),
        F.round(F.sum("event_at_percent_rank"), 2).alias("total_event_at_percent_rank"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
    )
)

with_windows = (
    summary
    .select(
        "*",
        F.sum("distinct_ticket_agent_ids").over(w_event_type).alias("event_type_sum_distinct_ticket_agent_ids"),
        F.dense_rank().over(w_ticket_agent_id_by_total_eve).alias("total_event_at_percent_rank_dense_rank"),
        F.round(F.col("total_event_at_percent_rank") / F.nullif(F.sum("total_event_at_percent_rank").over(w_event_type), F.lit(0)), 4).alias("total_event_at_percent_rank_share_of_event_type"),
        F.round(F.percent_rank().over(w_ticket_agent_id_by_total_eve_2), 3).alias("total_event_at_percent_rank_percent_rank"),
    )
)

result = (
    with_windows
    .filter(F.col("last_last_event_at") >= F.lit("2025-01-25 14:00:00").cast("timestamp"))
    .select(
        "total_event_at_percent_rank_dense_rank",
        "total_event_at_percent_rank_share_of_event_type",
        "total_event_at_percent_rank_percent_rank",
    )
    .distinct()
)
