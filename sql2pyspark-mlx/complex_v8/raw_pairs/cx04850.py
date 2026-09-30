from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_at_quarter").desc(), "id")
w_n_actor_not_in_system_agent = Window.partitionBy("n_actor_not_in_system_agent").orderBy("n_actor_not_in_system_agent", "actor")
w_n_actor_not_in_system_agent_2 = Window.partitionBy("n_actor_not_in_system_agent")
w_n_actor_not_in_system_agent_3 = Window.partitionBy("n_actor_not_in_system_agent").orderBy("n_actor_not_in_system_agent", "actor").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_or_unknown_actor_by_mi = Window.partitionBy("actor_or_unknown", "actor").orderBy(F.col("min_minutes_since_prev_event_at").desc())
w_actor_or_unknown_first_n_act = Window.partitionBy("actor_or_unknown", "first_n_actor_not_in_system_agent")
w_actor_or_unknown_actor_by_mi_2 = Window.partitionBy("actor_or_unknown", "actor").orderBy(F.col("min_minutes_since_prev_event_at").desc(), "actor").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_or_unknown_actor_by_mi_3 = Window.partitionBy("actor_or_unknown", "actor").orderBy(F.col("min_minutes_since_prev_event_at").desc(), "actor")

valid = (
    df_ticket_events
    .filter((F.col("event_type") == "created") | (F.col("actor").isin("customer", "agent")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("quarter", F.col("event_at")).alias("event_at_quarter"),
        F.date_trunc("day", F.col("event_at")).alias("event_at_day"),
    )
)

sequenced = (
    valid
    .select(
        "*",
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_ticket_id_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
    )
)

per_group = (
    sequenced
    .filter(F.hour(F.col("event_at")).between(10, 13))
    .groupBy("actor")
    .agg(
        F.round(F.sum("minutes_since_prev_event_at"), 2).alias("total_minutes_since_prev_event_at"),
        F.min("minutes_since_prev_event_at").alias("min_minutes_since_prev_event_at"),
        F.count(F.when(~F.col("actor").isin("system", "agent"), 1)).alias("n_actor_not_in_system_agent"),
        F.round(F.avg("minutes_since_prev_event_at"), 2).alias("avg_minutes_since_prev_event_at"),
    )
)

scored = (
    per_group
    .select(
        "*",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
        F.initcap(F.trim(F.col("actor"))).alias("actor_clean"),
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.dense_rank().over(w_n_actor_not_in_system_agent).alias("n_actor_not_in_system_agent_dense_rank"),
        F.sum("n_actor_not_in_system_agent").over(w_n_actor_not_in_system_agent_2).alias("n_actor_not_in_system_agent_sum_n_actor_not_in_s"),
        F.first("n_actor_not_in_system_agent").over(w_n_actor_not_in_system_agent_3).alias("first_n_actor_not_in_system_agent"),
    )
)

ranked = (
    with_windows
    .select(
        "*",
        F.rank().over(w_actor_or_unknown_actor_by_mi).alias("min_minutes_since_prev_event_at_rank"),
        F.round(F.col("n_actor_not_in_system_agent") / F.nullif(F.sum("n_actor_not_in_system_agent").over(w_actor_or_unknown_first_n_act), F.lit(0)), 4).alias("n_actor_not_in_system_agent_share_of_actor_or_un"),
        F.count(F.lit(1)).over(w_actor_or_unknown_first_n_act).alias("rows_in_actor_or_unknown_first_n_actor_not_in_sy"),
        F.last("n_actor_not_in_system_agent").over(w_actor_or_unknown_actor_by_mi_2).alias("last_n_actor_not_in_system_agent"),
        F.round(F.cume_dist().over(w_actor_or_unknown_actor_by_mi_3), 3).alias("min_minutes_since_prev_event_at_cume_dist"),
        F.size(F.collect_set("actor").over(w_actor_or_unknown_first_n_act)).alias("distinct_actor_in_actor_or_unknown_first_n_actor"),
    )
)

result = (
    ranked
    .groupBy("first_n_actor_not_in_system_agent", "last_n_actor_not_in_system_agent")
    .pivot("actor_or_unknown", ["agent", "system"])
    .agg(F.sum("total_minutes_since_prev_event_at"))
    .orderBy("first_n_actor_not_in_system_agent", "last_n_actor_not_in_system_agent")
)
