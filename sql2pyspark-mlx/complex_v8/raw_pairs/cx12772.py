from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_event_at_ties = Window.partitionBy("actor").orderBy("event_at")
w_actor_or_unknown_by_event_at = Window.partitionBy("actor_or_unknown").orderBy("event_at")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_actor_by_event_at = Window.partitionBy("actor").orderBy("event_at", "id")
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_at_rank = Window.partitionBy("event_at_rank")
w_event_at_quartile_event_at_t = Window.partitionBy("event_at_quartile", "event_at_tercile").orderBy(F.col("minutes_since_prev_event_at").asc_nulls_last())
w_event_at_quartile_by_event_a = Window.partitionBy("event_at_quartile").orderBy("event_at_rank_3", "id").rowsBetween(-6, Window.currentRow)
w_event_at_quartile_event_at_t_2 = Window.partitionBy("event_at_quartile", "event_at_tercile").orderBy(F.col("minutes_since_prev_event_at").asc_nulls_last(), "id")
w_actor_or_unknown = Window.partitionBy("actor_or_unknown")
w_all_by_pct_actor_is_not_null = Window.orderBy("pct_actor_is_not_null", "actor_or_unknown", "event_at_day")

base = (
    df_ticket_events
    .filter(~F.col("event_type").isin("created", "assigned"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("actor").isin("agent", "system"), "group_a").when(F.col("actor") == "customer", "group_b").otherwise("other").alias("actor_group"),
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

ranked = (
    base
    .select(
        "*",
        F.round(F.percent_rank().over(w_actor_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.rank().over(w_actor_or_unknown_by_event_at).alias("event_at_rank"),
        F.ntile(3).over(w_all_by_event_at).alias("event_at_tercile"),
        F.row_number().over(w_actor_by_event_at).alias("seq_no"),
        F.rank().over(w_actor_by_event_at).alias("event_at_rank_2"),
        F.ntile(4).over(w_all_by_event_at).alias("event_at_quartile"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.rank().over(w_event_type_by_event_at).alias("event_at_rank_3"),
    )
)

top_rows = (
    ranked
    .filter((F.col("event_at_rank_2") == 1) & (F.col("actor_or_unknown") == "system"))
    .drop("event_at_rank_2")
)

with_windows = (
    top_rows
    .select(
        "*",
        F.round(F.col("event_at_tercile") / F.nullif(F.sum("event_at_tercile").over(w_event_at_rank), F.lit(0)), 4).alias("event_at_tercile_share_of_event_at_rank"),
        F.rank().over(w_event_at_quartile_event_at_t).alias("minutes_since_prev_event_at_rank"),
        F.round(F.avg("event_at_tercile").over(w_event_at_quartile_by_event_a), 2).alias("event_at_tercile_avg_7"),
        F.round(F.cume_dist().over(w_event_at_quartile_event_at_t_2), 3).alias("minutes_since_prev_event_at_cume_dist"),
    )
)

totals = (
    with_windows
    .groupBy("actor_or_unknown", F.to_date("event_at").alias("event_at_day"))
    .agg(
        F.round(F.avg("minutes_since_prev_event_at_rank"), 2).alias("avg_minutes_since_prev_event_at_rank"),
        F.sum("minutes_since_prev_event_at_cume_dist").alias("total_minutes_since_prev_event_at_cume_dist"),
        F.round(100 * F.count(F.when(F.col("actor").isNotNull(), 1)) / F.count("*"), 1).alias("pct_actor_is_not_null"),
        F.max("minutes_since_prev_event_at_rank").alias("max_minutes_since_prev_event_at_rank_check"),
    )
    .filter(F.col("max_minutes_since_prev_event_at_rank_check") > 4)
    .drop("max_minutes_since_prev_event_at_rank_check")
)

with_trends = (
    totals
    .select(
        "*",
        F.sum("pct_actor_is_not_null").over(w_actor_or_unknown).alias("actor_or_unknown_sum_pct_actor_is_not_null"),
        F.lead("event_at_day", 2).over(w_all_by_pct_actor_is_not_null).alias("next2_event_at_day"),
        F.lead("pct_actor_is_not_null", 2).over(w_all_by_pct_actor_is_not_null).alias("next2_pct_actor_is_not_null"),
    )
)

result = (
    with_trends
    .select(
        "actor_or_unknown",
        "event_at_day",
        "avg_minutes_since_prev_event_at_rank",
        "pct_actor_is_not_null",
        "next2_event_at_day",
        F.lower(F.regexp_replace(F.trim(F.col("actor_or_unknown")), " ", "_")).alias("actor_or_unknown_slug"),
        F.date_format(F.col("next2_event_at_day"), "yyyy-MM-dd").alias("next2_event_at_day_label"),
    )
    .orderBy(
        F.col("avg_minutes_since_prev_event_at_rank").asc_nulls_last(),
        "next2_event_at_day",
    )
)
