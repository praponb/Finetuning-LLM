from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_by_ticket_closed_at_ru = Window.partitionBy("actor").orderBy("ticket_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_by_ticket_closed_at_ti = Window.partitionBy("actor").orderBy("ticket_closed_at")
w_event_type_ticket_id_by_even_2 = Window.partitionBy("event_type", "ticket_id").orderBy("event_at", "id")
w_actor_by_ticket_closed_at = Window.partitionBy("actor").orderBy("ticket_closed_at", "id")
w_actor_or_unknown_by_ticket_c = Window.partitionBy("actor_or_unknown").orderBy(F.col("ticket_closed_at").desc_nulls_first())
w_actor_or_unknown_by_ticket_c_2 = Window.partitionBy("actor_or_unknown").orderBy(F.col("ticket_closed_at").desc_nulls_first(), "id")

recent = (
    df_ticket_events
    .filter((F.col("ticket_id") <= 31) & (F.col("event_type").isin("assigned", "reopened")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

with_parent_ticket = (
    recent.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.actor_or_unknown",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

with_trends = (
    with_parent_ticket
    .select(
        "*",
        F.last("event_at", ignorenulls=True).over(w_event_type_ticket_id_by_even).alias("last_known_event_at"),
        F.last("event_at", ignorenulls=True).over(w_actor_by_ticket_closed_at_ru).alias("last_known_event_at_2"),
        F.round(F.cume_dist().over(w_actor_by_ticket_closed_at_ti), 3).alias("ticket_closed_at_cume_dist"),
        F.round((F.unix_timestamp("ticket_closed_at") - F.unix_timestamp(F.lag("ticket_closed_at").over(w_event_type_ticket_id_by_even_2))) / 60, 1).alias("minutes_since_prev_ticket_closed_at"),
        F.dense_rank().over(w_actor_by_ticket_closed_at).alias("ticket_closed_at_dense_rank"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_ticket_id_by_even_2))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.rank().over(w_actor_or_unknown_by_ticket_c).alias("ticket_closed_at_rank"),
        F.rank().over(w_event_type_ticket_id_by_even_2).alias("event_at_rank"),
        F.lead("event_at", 2).over(w_actor_or_unknown_by_ticket_c_2).alias("next2_event_at"),
        F.lag("event_at", 3).over(w_actor_or_unknown_by_ticket_c_2).alias("prev3_event_at"),
        F.lead("ticket_closed_at", 2).over(w_event_type_ticket_id_by_even_2).alias("next2_ticket_closed_at"),
        F.ntile(4).over(w_event_type_ticket_id_by_even_2).alias("event_at_quartile"),
        F.lead("ticket_closed_at").over(w_actor_or_unknown_by_ticket_c_2).alias("next_ticket_closed_at"),
        F.lead("event_at").over(w_event_type_ticket_id_by_even_2).alias("next_event_at"),
    )
)

first_rows = with_trends.filter(F.col("ticket_closed_at_dense_rank") == 1)

stats = (
    first_rows
    .filter(F.col("ticket_agent_id") >= 5)
    .groupBy(
        "actor",
        F.date_trunc("month", "last_known_event_at_2").alias("last_known_event_at_2_month"),
        "ticket_closed_at_rank",
    )
    .agg(
        F.min("last_known_event_at").alias("first_last_known_event_at"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.round(100 * F.count(F.when(F.col("event_type").isNotNull(), 1)) / F.count("*"), 1).alias("pct_event_type_is_not_null"),
    )
)

result = (
    stats
    .filter(F.col("ticket_closed_at_rank") < 8)
    .select(
        "actor",
        "last_known_event_at_2_month",
        "ticket_closed_at_rank",
        "event_type_list",
        "pct_event_type_is_not_null",
    )
)
