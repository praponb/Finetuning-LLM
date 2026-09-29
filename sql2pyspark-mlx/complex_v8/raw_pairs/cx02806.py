from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_rows_by_n_actor_is_not_nul = Window.partitionBy("n_rows").orderBy("n_actor_is_not_null", "actor").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_rows_by_n_actor_is_not_nul_2 = Window.partitionBy("n_rows").orderBy("n_actor_is_not_null", "actor")

filtered = (
    df_ticket_events
    .filter((~F.col("event_type").isin("resolved", "escalated")) & (F.col("actor").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

with_activity = (
    filtered.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isin("assigned", "reopened")), "left_anti")
)

annotated = (
    with_activity
    .filter(F.hour(F.col("event_at")).between(8, 13))
    .select(
        "*",
        F.concat_ws(": ", F.col("actor"), F.col("event_type_or_unknown")).alias("actor_event_type_or_unknown_label"),
        F.when(F.col("actor").isin("customer", "agent"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
    )
)

stats = (
    annotated
    .filter(F.col("ticket_id") >= 44)
    .groupBy("actor")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count("*").alias("n_rows"),
        F.count(F.when(F.col("actor").isNotNull(), 1)).alias("n_actor_is_not_null"),
    )
)

windowed = (
    stats
    .select(
        "*",
        F.first("last_event_at", ignorenulls=True).over(w_n_rows_by_n_actor_is_not_nul).alias("first_known_last_event_at"),
        F.last("n_actor_is_not_null", ignorenulls=True).over(w_n_rows_by_n_actor_is_not_nul).alias("last_known_n_actor_is_not_null"),
        F.lag("last_event_at").over(w_n_rows_by_n_actor_is_not_nul_2).alias("prev_last_event_at"),
    )
)

derived = (
    windowed
    .select(
        "actor",
        "n_rows",
        "n_actor_is_not_null",
        "first_known_last_event_at",
        "prev_last_event_at",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.substring(F.trim(F.col("actor")), 1, 3).alias("actor_prefix"),
    )
)

result = (
    derived
    .groupingSets(
        [["n_actor_is_not_null", "actor"], ["n_actor_is_not_null"], ["actor"], []],
        "n_actor_is_not_null", "actor",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.stddev("n_actor_is_not_null"), 2).alias("stddev_n_actor_is_not_null"),
    )
    .select(
        "n_actor_is_not_null",
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        "grouping_level",
        "stddev_n_actor_is_not_null",
    )
)
