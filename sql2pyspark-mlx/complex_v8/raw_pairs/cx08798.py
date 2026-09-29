from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").desc(), "id")
w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy("event_at")
w_ticket_id_actor_by_event_at_2 = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").desc())
w_event_type_actor_by_event_at_2 = Window.partitionBy("event_type", "actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_year_by_event_at_ti = Window.partitionBy(F.year("event_at")).orderBy("event_at")
w_event_at_year_by_event_at = Window.partitionBy(F.year("event_at")).orderBy("event_at", "id")
w_all_by_total_rn = Window.orderBy(F.col("total_rn").desc(), "event_at_rank", "actor")
w_event_at_rank_distinct_actor = Window.partitionBy("event_at_rank", "distinct_actors")

valid_events = (
    df_ticket_events
    .filter(F.col("event_type") != "replied")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_trends = (
    valid_events
    .select(
        "*",
        F.row_number().over(w_ticket_id_actor_by_event_at).alias("rn"),
        F.round(F.cume_dist().over(w_event_type_actor_by_event_at), 3).alias("event_at_cume_dist"),
        F.dense_rank().over(w_ticket_id_actor_by_event_at_2).alias("event_at_dense_rank"),
        F.round(F.percent_rank().over(w_ticket_id_actor_by_event_at_2), 3).alias("event_at_percent_rank"),
        F.nth_value("event_at", 2).over(w_event_type_actor_by_event_at_2).alias("second_event_at"),
        F.round(F.percent_rank().over(w_event_at_year_by_event_at_ti), 3).alias("event_at_percent_rank_2"),
        F.rank().over(w_event_type_actor_by_event_at).alias("event_at_rank"),
        F.lag("event_at").over(w_event_at_year_by_event_at).alias("prev_event_at"),
    )
)

grouped = (
    with_trends
    .groupBy("event_at_rank", "actor")
    .agg(
        F.sum("rn").alias("total_rn"),
        F.countDistinct("actor").alias("distinct_actors"),
        F.percentile_approx("event_at_percent_rank", 0.5).alias("median_event_at_percent_rank"),
        F.round(F.sum("event_at_cume_dist") / F.nullif(F.sum("event_at_dense_rank"), F.lit(0)), 3).alias("event_at_cume_dist_to_event_at_dense_rank_ratio"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("event_at_rank").alias("sum_event_at_rank_check"),
    )
    .filter(F.col("sum_event_at_rank_check") > 2)
    .drop("sum_event_at_rank_check")
)

sequenced = (
    grouped
    .select(
        "*",
        F.ntile(5).over(w_all_by_total_rn).alias("total_rn_quintile"),
        F.count(F.lit(1)).over(w_event_at_rank_distinct_actor).alias("rows_in_event_at_rank_distinct_actors"),
        F.round(F.col("event_at_rank") / F.nullif(F.sum("event_at_rank").over(w_event_at_rank_distinct_actor), F.lit(0)), 4).alias("event_at_rank_share_of_event_at_rank_distinct_ac"),
        F.max("distinct_actors").over(w_event_at_rank_distinct_actor).alias("event_at_rank_distinct_actors_max_distinct_actor"),
    )
)

result = (
    sequenced
    .select(
        "event_at_rank",
        "actor",
        "distinct_actors",
        "median_event_at_percent_rank",
        "event_at_cume_dist_to_event_at_dense_rank_ratio",
        "distinct_ids",
        "total_rn_quintile",
        "rows_in_event_at_rank_distinct_actors",
        "event_at_rank_distinct_actors_max_distinct_actor",
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
    )
    .orderBy(
        F.col("rows_in_event_at_rank_distinct_actors").desc(),
        "event_at_rank",
        "actor",
    )
    .limit(10)
)
