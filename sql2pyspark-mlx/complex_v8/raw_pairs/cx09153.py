from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_event_at_month_by_even = Window.partitionBy("actor", F.month("event_at")).orderBy("event_at", "id")
w_actor_event_at_month_by_even_2 = Window.partitionBy("actor", F.month("event_at")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_event_at_month_by_even_3 = Window.partitionBy("actor", F.month("event_at")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_event_at_month_by_even_4 = Window.partitionBy("actor", F.month("event_at")).orderBy("event_at")
w_event_at_percent_rank_tier_b = Window.partitionBy("event_at_percent_rank_tier").orderBy(F.col("event_at_percent_rank_actor_system").desc_nulls_first(), "event_at_percent_rank_tier", "actor").rowsBetween(-1, -1)
w_event_at_percent_rank_tier_b_2 = Window.partitionBy("event_at_percent_rank_tier").orderBy(F.col("event_at_percent_rank_actor_system").desc_nulls_first(), "event_at_percent_rank_tier", "actor").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_event_at_percent_rank = Window.partitionBy("actor", "event_at_percent_rank_tier").orderBy("n_actor_agent")
w_event_at_percent_rank_tier = Window.partitionBy("event_at_percent_rank_tier")
w_actor_event_at_percent_rank_2 = Window.partitionBy("actor", "event_at_percent_rank_tier").orderBy("n_actor_agent", "event_at_percent_rank_tier", "actor").rowsBetween(-2, -1)
w_event_at_percent_rank_tier_b_3 = Window.partitionBy("event_at_percent_rank_tier").orderBy(F.col("event_at_percent_rank_actor_system").desc_nulls_first())

valid_events = (
    df_ticket_events
    .filter((F.col("actor") != "customer") & (~F.col("event_type").isin("resolved", "escalated")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_windows = (
    valid_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        F.rank().over(w_actor_event_at_month_by_even).alias("event_at_rank"),
        F.first("event_at").over(w_actor_event_at_month_by_even_2).alias("first_event_at"),
        F.first("event_at", ignorenulls=True).over(w_actor_event_at_month_by_even_3).alias("first_known_event_at"),
        F.round(F.percent_rank().over(w_actor_event_at_month_by_even_4), 3).alias("event_at_percent_rank"),
    )
)

summary = (
    with_windows
    .groupBy(
        F.when(F.col("event_at_percent_rank") >= 0.7, "large").when(F.col("event_at_percent_rank") >= 0.3, "medium").otherwise("small").alias("event_at_percent_rank_tier"),
        "actor",
    )
    .agg(
        F.count(F.when(F.col("actor") == "agent", 1)).alias("n_actor_agent"),
        F.sum(F.when(F.col("actor") == "system", F.col("event_at_percent_rank"))).alias("event_at_percent_rank_actor_system"),
    )
)

with_trends = (
    summary
    .select(
        "*",
        F.max("n_actor_agent").over(w_event_at_percent_rank_tier_b).alias("n_actor_agent_max_1_prior"),
        F.nth_value("n_actor_agent", 2).over(w_event_at_percent_rank_tier_b_2).alias("second_n_actor_agent"),
        F.round(F.cume_dist().over(w_actor_event_at_percent_rank), 3).alias("n_actor_agent_cume_dist"),
        F.round(F.avg("n_actor_agent").over(w_event_at_percent_rank_tier), 2).alias("event_at_percent_rank_tier_avg_n_actor_agent"),
        F.sum("n_actor_agent").over(w_actor_event_at_percent_rank_2).alias("n_actor_agent_sum_2_prior"),
        F.dense_rank().over(w_event_at_percent_rank_tier_b_3).alias("event_at_percent_rank_actor_system_dense_rank"),
    )
)

result = (
    with_trends
    .filter(F.col("n_actor_agent_cume_dist") < 0.7)
    .select(
        "event_at_percent_rank_tier",
        "event_at_percent_rank_actor_system",
        "second_n_actor_agent",
        "n_actor_agent_cume_dist",
        "event_at_percent_rank_tier_avg_n_actor_agent",
        "n_actor_agent_sum_2_prior",
        "event_at_percent_rank_actor_system_dense_rank",
        F.round(F.col("second_n_actor_agent") / F.nullif(F.col("n_actor_agent"), F.lit(0)), 3).alias("second_n_actor_agent_per_n_actor_agent"),
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)
