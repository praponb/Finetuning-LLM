from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_new_session_median_burst = Window.partitionBy("is_new_session", "median_burst_id")
w_is_new_session_median_burst_2 = Window.partitionBy("is_new_session", "median_burst_id").orderBy(F.col("pct_actor_in_system_agent").desc(), "is_new_session")

base_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws("|", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

with_prev = base_events.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_ticket_id_running))

grouped = (
    sessionized
    .groupBy("is_new_session")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.sum(F.when(F.col("actor") == "agent", 1).otherwise(0)).alias("n_actor_agent"),
        F.round(100 * F.count(F.when(F.col("actor").isin("system", "agent"), 1)) / F.count("*"), 1).alias("pct_actor_in_system_agent"),
        F.percentile_approx("burst_id", 0.5).alias("median_burst_id"),
        F.min("is_new_session").alias("min_is_new_session"),
        F.sum("is_new_session").alias("total_is_new_session"),
    )
)

with_trends = (
    grouped
    .withColumn("rows_in_is_new_session_median_burst_id", F.count(F.lit(1)).over(w_is_new_session_median_burst))
    .withColumn("pct_actor_in_system_agent_cume_dist", F.round(F.cume_dist().over(w_is_new_session_median_burst_2), 3))
)

result = (
    with_trends
    .groupingSets(
        [["total_is_new_session", "median_burst_id", "min_is_new_session"], ["total_is_new_session"], ["median_burst_id"], []],
        "total_is_new_session", "median_burst_id", "min_is_new_session",
    )
    .agg(
        F.grouping("total_is_new_session").alias("is_total_total_is_new_session"),
        F.min("is_new_session").alias("min_is_new_session_2"),
        F.min("pct_actor_in_system_agent").alias("min_pct_actor_in_system_agent"),
    )
)
