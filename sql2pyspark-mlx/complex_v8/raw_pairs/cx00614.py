from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_year_ties = Window.orderBy("event_at_year")
w_event_at_year_actor_by_event = Window.partitionBy("event_at_year", "actor").orderBy("event_at", "id").rowsBetween(-2, Window.currentRow)
w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at_year_rank").asc_nulls_last())
w_actor_event_type_by_event_at_2 = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at_year_rank").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_actor_by_event_at_moving_2 = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(-2, Window.currentRow)
w_event_at_year_event_at_year = Window.partitionBy(F.year("event_at"), "event_at_year")
w_actor_by_event_at_ties = Window.partitionBy("actor").orderBy("event_at")

recent = (
    df_ticket_events
    .filter((F.col("actor") == "agent") & (F.col("event_type") == "created"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.year(F.col("event_at")).alias("event_at_year"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
    )
)

ranked = (
    recent
    .withColumn("event_at_year_rank", F.rank().over(w_all_by_event_at_year_ties))
    .withColumn("event_at_year_avg_3", F.round(F.avg("event_at_year").over(w_event_at_year_actor_by_event), 2))
)

with_windows = (
    ranked
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "event_at_year",
        "event_at_label",
        "event_at_year_rank",
        F.dense_rank().over(w_actor_event_type_by_event_at).alias("event_at_year_rank_dense_rank"),
        F.sum("event_at_year").over(w_actor_event_type_by_event_at_2).alias("event_at_year_sum_7"),
        F.max("event_at_year_rank").over(w_actor_by_event_at_moving_2).alias("event_at_year_rank_max_3"),
        F.round(F.avg("event_at_year_rank").over(w_event_at_year_event_at_year), 2).alias("event_at_year_event_at_year_avg_event_at_year_ra"),
        F.rank().over(w_actor_by_event_at_ties).alias("event_at_rank"),
        F.size(F.collect_set("actor").over(w_event_at_year_event_at_year)).alias("distinct_actor_in_event_at_year_event_at_year"),
    )
)

stats = (
    with_windows
    .groupBy("event_at_year_rank_dense_rank")
    .agg(
        F.sum("event_at_year").alias("total_event_at_year"),
        F.count("*").alias("n_rows"),
        F.sum(F.when(F.col("actor") == "customer", 1).otherwise(0)).alias("n_actor_customer"),
        F.round(F.sum("event_at_year") / F.nullif(F.sum("event_at_year_rank"), F.lit(0)), 3).alias("event_at_year_to_event_at_year_rank_ratio"),
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
        F.avg("event_at_year_rank_max_3").alias("avg_event_at_year_rank_max_3_check"),
    )
    .filter(F.col("avg_event_at_year_rank_max_3_check") > 10)
    .drop("avg_event_at_year_rank_max_3_check")
)

without_matches = (
    stats
    .crossJoin(stats.agg(F.min("n_rows").alias("overall_min_n_rows")))
    .filter(F.col("n_rows") > F.col("overall_min_n_rows"))
    .drop("overall_min_n_rows")
)

result = (
    without_matches
    .groupingSets(
        [["distinct_ticket_ids", "event_at_year_rank_dense_rank"], ["distinct_ticket_ids"]],
        "distinct_ticket_ids", "event_at_year_rank_dense_rank",
    )
    .agg(
        F.min("total_event_at_year").alias("min_total_event_at_year"),
        F.round(F.stddev("event_at_year_rank_dense_rank"), 2).alias("stddev_event_at_year_rank_dense_rank"),
        F.min("event_at_year_to_event_at_year_rank_ratio").alias("min_event_at_year_to_event_at_year_rank_ratio"),
    )
    .orderBy(
        F.col("distinct_ticket_ids").asc_nulls_last(),
        F.col("event_at_year_rank_dense_rank").asc_nulls_last(),
    )
)
