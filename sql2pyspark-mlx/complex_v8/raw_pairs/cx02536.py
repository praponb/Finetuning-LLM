from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_by_event_at_runni = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy("event_at")
w_dedup = Window.partitionBy("actor").orderBy(F.col("event_at_cume_dist").desc(), F.col("id").desc())
w_event_at_rank_actor = Window.partitionBy("event_at_rank", "actor")
w_event_at_rank_by_last_known = Window.partitionBy("event_at_rank").orderBy("last_known_event_at", "id")
w_event_at_rank_by_last_known_2 = Window.partitionBy("event_at_rank").orderBy("last_known_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_rank_last_known_eve = Window.partitionBy("event_at_rank", F.month("last_known_event_at")).orderBy("event_at_rank", "id").rowsBetween(-2, -1)

valid = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) | (F.col("actor").isin("system", "agent")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

metrics = (
    valid
    .select(
        "id",
        "actor",
        F.round(F.percent_rank().over(w_event_type_by_event_at), 3).alias("event_at_percent_rank"),
        F.last("event_at", ignorenulls=True).over(w_event_type_by_event_at_runni).alias("last_known_event_at"),
        F.round(F.cume_dist().over(w_event_type_by_event_at), 3).alias("event_at_cume_dist"),
        F.rank().over(w_event_type_by_event_at_ties).alias("event_at_rank"),
    )
)

without_matches = (
    metrics
    .crossJoin(metrics.agg(F.avg("event_at_rank").alias("overall_avg_event_at_rank")))
    .filter(F.col("event_at_rank") > F.col("overall_avg_event_at_rank"))
    .drop("overall_avg_event_at_rank")
)

latest = (
    without_matches
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

windowed = (
    latest
    .select(
        "*",
        F.count(F.lit(1)).over(w_event_at_rank_actor).alias("rows_in_event_at_rank_actor"),
        F.round(F.percent_rank().over(w_event_at_rank_by_last_known), 3).alias("last_known_event_at_percent_rank"),
        F.nth_value("event_at_rank", 2).over(w_event_at_rank_by_last_known_2).alias("second_event_at_rank"),
        F.round(F.avg("event_at_cume_dist").over(w_event_at_rank_last_known_eve), 2).alias("event_at_cume_dist_avg_2_prior"),
    )
)

result = (
    windowed
    .filter(F.col("actor").isin("system", "agent"))
    .select(
        "id",
        "actor",
        "event_at_cume_dist",
        "rows_in_event_at_rank_actor",
        "last_known_event_at_percent_rank",
        "second_event_at_rank",
        "event_at_cume_dist_avg_2_prior",
        F.when(F.col("actor").isin("agent", "system"), "group_a").when(F.col("actor") == "customer", "group_b").otherwise("other").alias("actor_group"),
        F.abs(F.col("second_event_at_rank") - 4).alias("second_event_at_rank_distance"),
    )
    .orderBy(
        F.col("last_known_event_at_percent_rank").desc(),
        "rows_in_event_at_rank_actor",
        "id",
    )
    .limit(5)
)
