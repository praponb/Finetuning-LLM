from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_event_type_is_not_null_by = Window.partitionBy("n_event_type_is_not_null").orderBy(F.col("n_event_type_is_not_null_2").desc_nulls_first(), "actor")
w_n_event_type_is_not_null_2_l = Window.partitionBy("n_event_type_is_not_null_2", F.year("last_event_at"))
w_n_actor_in_agent_system_by_n = Window.partitionBy("n_actor_in_agent_system").orderBy(F.col("n_event_type_is_not_null_2").asc_nulls_last(), "actor").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_actor_in_agent_system_by_n_2 = Window.partitionBy("n_actor_in_agent_system").orderBy(F.col("n_event_type_is_not_null_2").asc_nulls_last(), "actor")
w_n_event_type_is_not_null_by_2 = Window.partitionBy("n_event_type_is_not_null").orderBy(F.col("n_event_type_is_not_null_2").desc_nulls_first(), "actor").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_ticket_events
    .filter((F.col("actor") == "system") & (F.col("ticket_id") <= 59))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(": ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
)

summary = (
    base
    .filter(F.col("event_type") == "replied")
    .groupBy("actor")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.sum(F.when(F.col("event_type").isNotNull(), 1).otherwise(0)).alias("n_event_type_is_not_null"),
        F.count(F.when(F.col("actor").isNotNull(), 1)).alias("n_actor_is_not_null"),
        F.count(F.when(F.col("event_type").isNotNull(), 1)).alias("n_event_type_is_not_null_2"),
        F.round(100 * F.count(F.when(F.col("actor") == "agent", 1)) / F.count("*"), 1).alias("pct_actor_agent"),
        F.sum(F.when(F.col("actor").isin("agent", "system"), 1).otherwise(0)).alias("n_actor_in_agent_system"),
    )
)

with_trends = (
    summary
    .select(
        "actor",
        "last_event_at",
        "n_event_type_is_not_null",
        "n_actor_is_not_null",
        "n_event_type_is_not_null_2",
        "pct_actor_agent",
        "n_actor_in_agent_system",
        (F.col("n_event_type_is_not_null") - F.lag("n_event_type_is_not_null").over(w_n_event_type_is_not_null_by)).alias("n_event_type_is_not_null_change"),
        F.sum("n_actor_in_agent_system").over(w_n_event_type_is_not_null_2_l).alias("n_event_type_is_not_null_2_last_event_at_year_su"),
        F.last("n_event_type_is_not_null_2").over(w_n_actor_in_agent_system_by_n).alias("last_n_event_type_is_not_null_2"),
        F.min("pct_actor_agent").over(w_n_event_type_is_not_null_2_l).alias("n_event_type_is_not_null_2_last_event_at_year_mi"),
        F.dense_rank().over(w_n_actor_in_agent_system_by_n_2).alias("n_event_type_is_not_null_2_dense_rank"),
        F.last("pct_actor_agent").over(w_n_event_type_is_not_null_by_2).alias("last_pct_actor_agent"),
    )
)

scored = (
    with_trends
    .select(
        "*",
        (F.floor(F.col("last_pct_actor_agent") / 25) * 25).alias("last_pct_actor_agent_bucket"),
        F.round(F.col("pct_actor_agent") * 1.07, 2).alias("pct_actor_agent_adj"),
        F.year(F.col("last_event_at")).alias("last_event_at_year"),
    )
)

top_rows = scored.filter(F.col("n_event_type_is_not_null_2_dense_rank") == 1)

result = (
    top_rows
    .groupBy("n_actor_is_not_null")
    .pivot("actor", ["system", "customer"])
    .agg(F.avg("pct_actor_agent"))
    .orderBy("n_actor_is_not_null")
)
