from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_actor_is_not_null_n_event = Window.partitionBy("n_actor_is_not_null", "n_event_type_assigned").orderBy("actor_len", "event_type", "actor").rowsBetween(-2, Window.currentRow)
w_n_actor_is_not_null_n_event_2 = Window.partitionBy("n_actor_is_not_null", "n_event_type_assigned").orderBy("actor_len", "event_type", "actor").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_actor_is_not_null_n_event_3 = Window.partitionBy("n_actor_is_not_null", "n_event_type_assigned").orderBy("actor_len", "event_type", "actor").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_actor = Window.partitionBy("event_type", "actor")

scoped = (
    df_ticket_events
    .filter((F.col("actor").isin("agent", "system")) | (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

stats = (
    scoped
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-01-31").cast("date"))
    .groupBy("event_type", "actor")
    .agg(
        F.sum(F.when(F.col("event_type") == "assigned", 1).otherwise(0)).alias("n_event_type_assigned"),
        F.count(F.when(~F.col("actor").isin("system", "customer"), 1)).alias("n_actor_not_in_system_customer"),
        F.count(F.when(F.col("actor").isNotNull(), 1)).alias("n_actor_is_not_null"),
        F.min("event_at").alias("first_event_at"),
        F.round(100 * F.count(F.when(F.col("actor") != "agent", 1)) / F.count("*"), 1).alias("pct_actor_agent"),
    )
)

scored = (
    stats
    .filter(F.col("first_event_at") >= F.lit("2025-01-25 14:00:00").cast("timestamp"))
    .select(
        "*",
        F.concat_ws(" / ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        (F.col("pct_actor_agent") + F.col("n_actor_is_not_null")).alias("pct_actor_agent_plus_n_actor_is_not_null"),
    )
)

sequenced = (
    scored
    .select(
        "*",
        F.round(F.avg("actor_len").over(w_n_actor_is_not_null_n_event), 2).alias("actor_len_avg_3"),
        F.count(F.lit(1)).over(w_n_actor_is_not_null_n_event_2).alias("running_count"),
        F.nth_value("pct_actor_agent", 2).over(w_n_actor_is_not_null_n_event_3).alias("second_pct_actor_agent"),
        F.min("pct_actor_agent").over(w_event_type_actor).alias("event_type_actor_min_pct_actor_agent"),
    )
)

qualified = (
    sequenced
    .crossJoin(sequenced.agg(F.max("pct_actor_agent_plus_n_actor_is_not_null").alias("overall_max_pct_actor_agent_plus_n_actor_is_not_")))
    .filter(F.col("pct_actor_agent_plus_n_actor_is_not_null") < F.col("overall_max_pct_actor_agent_plus_n_actor_is_not_"))
    .drop("overall_max_pct_actor_agent_plus_n_actor_is_not_")
)

result = (
    qualified
    .select(
        "event_type",
        "actor",
        "n_event_type_assigned",
        "n_actor_is_not_null",
        "actor_event_type_label",
        "actor_len",
        "actor_len_avg_3",
        "running_count",
        "second_pct_actor_agent",
        "event_type_actor_min_pct_actor_agent",
        F.concat(F.col("event_type"), F.lit("#"), F.col("n_event_type_assigned").cast("string")).alias("event_type_ref"),
        F.date_format(F.col("first_event_at"), "yyyy-MM-dd").alias("first_event_at_label"),
    )
    .orderBy(
        F.col("event_type_actor_min_pct_actor_agent").asc_nulls_last(),
        F.col("actor_len").desc(),
    )
)
