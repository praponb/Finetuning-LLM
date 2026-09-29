from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_event_at_full = Window.partitionBy("actor").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_by_event_at = Window.partitionBy("actor").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_actor_by_event_at_ties = Window.partitionBy("actor").orderBy(F.col("event_at").asc_nulls_last())

scoped = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(": ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
    )
)

with_trends = (
    scoped
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "actor_event_type_label",
        "event_at_label",
        F.nth_value("event_at", 2).over(w_actor_by_event_at_full).alias("second_event_at"),
        F.rank().over(w_actor_by_event_at).alias("event_at_rank"),
        F.round(F.cume_dist().over(w_actor_by_event_at_ties), 3).alias("event_at_cume_dist"),
    )
)

rollup_base = (
    with_trends
    .groupBy("event_at_rank")
    .agg(
        F.round(F.sum("event_at_rank") / F.nullif(F.sum("event_at_cume_dist"), F.lit(0)), 3).alias("event_at_rank_to_event_at_cume_dist_ratio"),
        F.sum(F.when(F.col("actor") == "customer", 1).otherwise(0)).alias("n_actor_customer"),
        F.count("*").alias("n_rows"),
        F.max("event_at_rank").alias("max_event_at_rank"),
        F.sum(F.when(F.col("actor") == "agent", F.col("event_at_cume_dist"))).alias("event_at_cume_dist_actor_agent"),
    )
)

filtered = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("max_event_at_rank").alias("overall_avg_max_event_at_rank")))
    .filter(F.col("max_event_at_rank") > F.col("overall_avg_max_event_at_rank"))
    .drop("overall_avg_max_event_at_rank")
)

result = (
    filtered
    .groupingSets(
        [["max_event_at_rank", "n_rows", "n_actor_customer"], ["max_event_at_rank"], []],
        "max_event_at_rank", "n_rows", "n_actor_customer",
    )
    .agg(
        F.round(F.stddev("event_at_cume_dist_actor_agent"), 2).alias("stddev_event_at_cume_dist_actor_agent"),
        F.sum("n_actor_customer").alias("total_n_actor_customer"),
        F.round(F.stddev("n_actor_customer"), 2).alias("stddev_n_actor_customer"),
    )
    .orderBy(
        F.col("max_event_at_rank").asc_nulls_last(),
        F.col("n_rows").asc_nulls_last(),
        F.col("n_actor_customer").asc_nulls_last(),
    )
)
