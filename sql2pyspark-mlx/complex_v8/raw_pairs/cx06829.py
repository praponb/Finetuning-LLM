from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_actor_group_by_actor_l = Window.partitionBy("actor", "actor_group").orderBy(F.col("actor_len").desc(), "id")
w_actor_group_by_actor_len = Window.partitionBy("actor_group").orderBy(F.col("actor_len").desc(), "id")
w_actor_len = Window.partitionBy("actor_len")
w_actor_group = Window.partitionBy("actor_group")
w_prev_actor_len_distinct_ids = Window.partitionBy("prev_actor_len", "distinct_ids").orderBy("total_actor_len_cume_dist", "actor_group", "prev_actor_len")
w_prev_actor_len_distinct_ids_2 = Window.partitionBy("prev_actor_len", "distinct_ids").orderBy("total_actor_len_cume_dist", "actor_group", "prev_actor_len").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_last_event_at_prior_2 = Window.orderBy(F.col("last_event_at").desc(), "actor_group", "prev_actor_len").rowsBetween(-2, -1)
w_all_by_distinct_ids_avg_2_pr = Window.orderBy(F.col("distinct_ids_avg_2_prior").asc_nulls_last(), "actor_group", "prev_actor_len").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_group_by_actor_group_m = Window.partitionBy("actor_group").orderBy("actor_group_min_distinct_ids")
w_prev_actor_len_actor_group = Window.partitionBy("prev_actor_len", "actor_group")
w_actor_group_by_actor_group_m_2 = Window.partitionBy("actor_group").orderBy("actor_group_min_distinct_ids", "actor_group", "prev_actor_len")

filtered_events = df_ticket_events.filter(F.col("ticket_id") >= 5)

scored = (
    filtered_events
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("actor").isin("customer", "agent"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "event_at",
        "actor_group",
        "actor_slug",
        F.lag("actor_len").over(w_actor_actor_group_by_actor_l).alias("prev_actor_len"),
        F.round(F.cume_dist().over(w_actor_group_by_actor_len), 3).alias("actor_len_cume_dist"),
        F.count(F.lit(1)).over(w_actor_len).alias("rows_in_actor_len"),
        F.min("actor_len").over(w_actor_len).alias("actor_len_min_actor_len"),
        F.round(F.col("actor_len") / F.nullif(F.sum("actor_len").over(w_actor_len), F.lit(0)), 4).alias("actor_len_share_of_actor_len"),
    )
)

rollup_base = (
    with_windows
    .filter(F.col("rows_in_actor_len") > 3)
    .groupBy("actor_group", "prev_actor_len")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.max("event_at").alias("last_event_at"),
        F.sum("actor_len_cume_dist").alias("total_actor_len_cume_dist"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

qualified = (
    rollup_base
    .crossJoin(rollup_base.agg(F.min("prev_actor_len").alias("overall_min_prev_actor_len")))
    .filter(F.col("prev_actor_len") > F.col("overall_min_prev_actor_len"))
    .drop("overall_min_prev_actor_len")
)

windowed = (
    qualified
    .select(
        "actor_group",
        "prev_actor_len",
        "first_event_at",
        "last_event_at",
        "total_actor_len_cume_dist",
        F.min("distinct_ids").over(w_actor_group).alias("actor_group_min_distinct_ids"),
        F.row_number().over(w_prev_actor_len_distinct_ids).alias("total_actor_len_cume_dist_seq"),
        F.min("distinct_ids").over(w_prev_actor_len_distinct_ids_2).alias("min_distinct_ids_so_far"),
        F.round(F.avg("distinct_ids").over(w_all_by_last_event_at_prior_2), 2).alias("distinct_ids_avg_2_prior"),
    )
)

metrics = (
    windowed
    .select(
        "*",
        F.sum("min_distinct_ids_so_far").over(w_all_by_distinct_ids_avg_2_pr).alias("running_min_distinct_ids_so_far"),
        F.rank().over(w_actor_group_by_actor_group_m).alias("actor_group_min_distinct_ids_rank"),
        F.round(F.avg("prev_actor_len").over(w_prev_actor_len_actor_group), 2).alias("prev_actor_len_actor_group_avg_prev_actor_len"),
        F.round(F.avg("min_distinct_ids_so_far").over(w_prev_actor_len_actor_group), 2).alias("prev_actor_len_actor_group_avg_min_distinct_ids_"),
        F.row_number().over(w_actor_group_by_actor_group_m_2).alias("rn"),
    )
)

result = (
    metrics
    .select(
        "actor_group",
        "prev_actor_len",
        "first_event_at",
        "total_actor_len_cume_dist",
        "total_actor_len_cume_dist_seq",
        "min_distinct_ids_so_far",
        "distinct_ids_avg_2_prior",
        "actor_group_min_distinct_ids_rank",
        "prev_actor_len_actor_group_avg_prev_actor_len",
        "prev_actor_len_actor_group_avg_min_distinct_ids_",
        F.round(F.col("distinct_ids_avg_2_prior") * 1.07, 2).alias("distinct_ids_avg_2_prior_adj"),
        F.round(F.col("distinct_ids_avg_2_prior") * 1.1, 2).alias("distinct_ids_avg_2_prior_adj_2"),
    )
    .orderBy("distinct_ids_avg_2_prior_adj_2")
)
