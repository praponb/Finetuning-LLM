from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy("event_at", "id")
w_event_type_by_event_at_full = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_actor_by_event_at_2 = Window.partitionBy("event_type", "actor").orderBy("event_at")

scoped = (
    df_ticket_events
    .filter((~F.col("event_type").isin("resolved", "replied")) & (F.col("ticket_id") >= 2))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("resolved", "reopened"), "group_a").when(F.col("event_type") == "assigned", "group_b").otherwise("other").alias("event_type_group"),
        F.concat_ws("-", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
    )
)

with_trends = (
    scoped
    .select(
        "*",
        F.ntile(10).over(w_event_type_actor_by_event_at).alias("event_at_decile"),
        F.nth_value("event_at", 2).over(w_event_type_by_event_at_full).alias("second_event_at"),
        F.lead("event_at").over(w_event_type_actor_by_event_at).alias("next_event_at"),
        F.rank().over(w_event_type_actor_by_event_at_2).alias("event_at_rank"),
    )
)

without_matches = (
    with_trends.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (~F.col("t.actor").isin("system", "agent")) & (F.to_date(F.col("t.event_at")) < F.lit("2025-03-08").cast("date")), "left_anti")
)

with_parent = (
    without_matches.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.requester_id") > 7)
    .select("s.*", F.col("t.requester_id").alias("ticket_requester_id"))
)

result = (
    with_parent
    .cube("event_at_rank", "actor")
    .agg(F.grouping_id().alias("gid"), F.count("*").alias("n_rows"))
    .orderBy(
        F.col("event_at_rank").asc_nulls_last(),
        F.col("actor").asc_nulls_last(),
    )
)
