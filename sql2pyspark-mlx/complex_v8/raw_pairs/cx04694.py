from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy("event_at", "id")

valid_events = (
    df_ticket_events
    .filter((F.col("event_type").isNotNull()) & (F.col("actor").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(": ", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        F.concat_ws("|", F.col("event_type"), F.col("actor")).alias("event_type_actor_label_2"),
    )
)

enriched = (
    valid_events
    .filter(F.col("event_type") != "resolved")
    .select(
        "*",
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label_2"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_type_actor_label_2",
        F.ntile(5).over(w_event_type_by_event_at).alias("event_at_quintile"),
        F.lead("event_at").over(w_event_type_actor_by_event_at).alias("next_event_at"),
    )
)

derived = (
    with_windows
    .select(
        "*",
        F.coalesce(F.col("event_at_quintile"), F.lit(0)).alias("event_at_quintile_filled"),
    )
)

result = (
    derived
    .groupingSets(
        [["actor", "event_at_quintile"], ["actor"], ["event_at_quintile"]],
        "actor", "event_at_quintile",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.count("*").alias("n_rows"),
        F.sum(F.when(F.col("event_type").isin("resolved", "reopened", "created"), F.col("event_at_quintile_filled")).otherwise(0)).alias("event_at_quintile_filled_event_type_in_resolved_"),
    )
)
