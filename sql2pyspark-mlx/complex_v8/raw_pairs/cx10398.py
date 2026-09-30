from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_to_date_by_event_at = Window.partitionBy(F.to_date("event_at")).orderBy(F.col("event_at").desc_nulls_first())
w_event_at_to_date_event_type = Window.partitionBy(F.to_date("event_at"), "event_type").orderBy(F.col("event_at").desc(), "id")
w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), "id")
w_event_at_to_date_by_event_at_2 = Window.partitionBy(F.to_date("event_at")).orderBy(F.col("event_at").desc_nulls_first(), "id")
w_actor_event_at_to_date_by_ev = Window.partitionBy("actor", F.to_date("event_at")).orderBy(F.col("event_at").desc(), "id")
w_event_at_month_actor_by_even = Window.partitionBy(F.month("event_at"), "actor").orderBy(F.col("event_at").desc(), "id")
w_event_at_to_date_event_type_2 = Window.partitionBy(F.to_date("event_at"), "event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_to_date_by_event_at_3 = Window.partitionBy(F.to_date("event_at")).orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_tercile_event_at_se = Window.partitionBy("event_at_tercile", "event_at_seq").orderBy(F.col("event_at_cume_dist").asc_nulls_last(), "id").rowsBetween(-1, -1)
w_event_at_dense_rank_2_ticket = Window.partitionBy("event_at_dense_rank_2", "ticket_id")
w_all_by_event_at_cume_dist_pr = Window.orderBy(F.col("event_at_cume_dist").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_ticket_id_by_event_at_dense = Window.partitionBy("ticket_id").orderBy(F.col("event_at_dense_rank").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_by_event_at_dense_2 = Window.partitionBy("ticket_id").orderBy(F.col("event_at_dense_rank").desc_nulls_first(), "id")

base_events = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("actor").isin("agent", "system")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

windowed = (
    base_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        F.dense_rank().over(w_event_at_to_date_by_event_at).alias("event_at_dense_rank"),
        F.ntile(3).over(w_event_at_to_date_event_type).alias("event_at_tercile"),
        F.row_number().over(w_event_type_actor_by_event_at).alias("event_at_seq"),
        F.round(F.percent_rank().over(w_event_at_to_date_by_event_at), 3).alias("event_at_percent_rank"),
        F.lead("event_at").over(w_event_at_to_date_by_event_at_2).alias("next_event_at"),
        F.round(F.cume_dist().over(w_actor_event_at_to_date_by_ev), 3).alias("event_at_cume_dist"),
        F.ntile(3).over(w_event_at_month_actor_by_even).alias("event_at_tercile_2"),
        F.first("event_at").over(w_event_at_to_date_event_type_2).alias("first_event_at"),
        F.first("event_at", ignorenulls=True).over(w_event_at_to_date_by_event_at_3).alias("first_known_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_at_month_actor_by_even))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.dense_rank().over(w_event_at_to_date_event_type).alias("event_at_dense_rank_2"),
    )
)

with_trends = (
    windowed
    .select(
        "*",
        F.sum("event_at_cume_dist").over(w_event_at_tercile_event_at_se).alias("event_at_cume_dist_sum_1_prior"),
        F.round(F.avg("event_at_tercile").over(w_event_at_dense_rank_2_ticket), 2).alias("event_at_dense_rank_2_ticket_id_avg_event_at_ter"),
        F.round(F.avg("event_at_tercile_2").over(w_all_by_event_at_cume_dist_pr), 2).alias("event_at_tercile_2_avg_2_prior"),
        F.max("event_at_tercile_2").over(w_ticket_id_by_event_at_dense).alias("max_event_at_tercile_2_so_far"),
        F.round(F.col("event_at_seq") / F.nullif(F.sum("event_at_seq").over(w_event_at_dense_rank_2_ticket), F.lit(0)), 4).alias("event_at_seq_share_of_event_at_dense_rank_2_tick"),
        F.ntile(5).over(w_ticket_id_by_event_at_dense_2).alias("event_at_dense_rank_quintile"),
        F.rank().over(w_ticket_id_by_event_at_dense_2).alias("event_at_dense_rank_rank"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "ticket_id",
        "event_type",
        "event_at_dense_rank",
        "event_at_tercile",
        "event_at_seq",
        "next_event_at",
        "event_at_tercile_2",
        "first_event_at",
        "first_known_event_at",
        "minutes_since_prev_event_at",
        "event_at_cume_dist_sum_1_prior",
        "event_at_dense_rank_2_ticket_id_avg_event_at_ter",
        "max_event_at_tercile_2_so_far",
        "event_at_seq_share_of_event_at_dense_rank_2_tick",
        "event_at_dense_rank_quintile",
        "event_at_dense_rank_rank",
        F.abs(F.col("event_at_tercile_2") - 7).alias("event_at_tercile_2_distance"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("event_at_dense_rank").cast("string")).alias("event_type_ref"),
    )
    .orderBy(
        F.col("event_at_dense_rank").desc(),
        F.col("event_at_dense_rank_quintile").asc_nulls_last(),
        "id",
    )
    .limit(10)
)
