from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_last_event_at = Window.orderBy(F.col("last_event_at").asc_nulls_last(), "event_type", "actor")
w_distinct_actors = Window.partitionBy("distinct_actors")
w_all_by_n_actor_system = Window.orderBy(F.col("n_actor_system").desc(), "event_type", "actor")

cleaned = (
    df_ticket_events
    .filter(F.col("event_type") == "assigned")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("actor").isin("system", "customer"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group"),
        F.concat_ws("|", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
)

stats = (
    cleaned
    .groupBy("event_type", "actor")
    .agg(
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("actor").isin("customer", "system"), 1).otherwise(0)).alias("n_actor_in_customer_system"),
        F.count(F.when(F.col("actor") != "system", 1)).alias("n_actor_system"),
        F.countDistinct("actor").alias("distinct_actors"),
        F.max("event_at").alias("last_event_at"),
    )
)

with_windows = (
    stats
    .select(
        "event_type",
        "actor",
        "actor_list",
        "cnt",
        "n_actor_in_customer_system",
        "n_actor_system",
        "last_event_at",
        F.ntile(5).over(w_all_by_last_event_at).alias("last_event_at_quintile"),
        F.min("distinct_actors").over(w_distinct_actors).alias("distinct_actors_min_distinct_actors"),
        F.lag("last_event_at", 3).over(w_all_by_n_actor_system).alias("prev3_last_event_at"),
        F.round(F.col("n_actor_system") - F.avg("n_actor_system").over(w_distinct_actors), 2).alias("n_actor_system_vs_distinct_actors_avg"),
        F.size(F.collect_set("actor").over(w_distinct_actors)).alias("distinct_actor_in_distinct_actors"),
    )
)

exploded = (
    with_windows
    .select(
        "event_type",
        "actor",
        "cnt",
        "n_actor_in_customer_system",
        "n_actor_system",
        "last_event_at",
        "last_event_at_quintile",
        "distinct_actors_min_distinct_actors",
        "prev3_last_event_at",
        "n_actor_system_vs_distinct_actors_avg",
        "distinct_actor_in_distinct_actors",
        F.posexplode("actor_list").alias("actor_list_item_pos", "actor_list_item"),
    )
)

screened = (
    exploded
    .crossJoin(exploded.agg(F.avg("distinct_actors_min_distinct_actors").alias("overall_avg_distinct_actors_min_distinct_actors")))
    .filter(F.col("distinct_actors_min_distinct_actors") > F.col("overall_avg_distinct_actors_min_distinct_actors"))
    .drop("overall_avg_distinct_actors_min_distinct_actors")
)

result = (
    screened
    .select(
        "event_type",
        "actor",
        "cnt",
        "n_actor_in_customer_system",
        "n_actor_system",
        "last_event_at",
        "last_event_at_quintile",
        "distinct_actors_min_distinct_actors",
        "prev3_last_event_at",
        "n_actor_system_vs_distinct_actors_avg",
        "actor_list_item",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.abs(F.col("n_actor_system") - 3).alias("n_actor_system_distance"),
    )
    .orderBy(F.col("cnt").desc())
)
