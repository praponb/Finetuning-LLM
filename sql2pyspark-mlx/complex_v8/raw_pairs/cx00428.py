from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_moving_1 = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_event_at_prior_2 = Window.orderBy("event_at", "id").rowsBetween(-2, -1)
w_actor_ticket_id = Window.partitionBy("actor", "ticket_id")
w_ticket_csat_ticket_id = Window.partitionBy("ticket_csat", "ticket_id")
w_actor_by_ticket_csat = Window.partitionBy("actor").orderBy(F.col("ticket_csat").desc(), "id")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_ticket_id_by_ticket_csat_mov = Window.partitionBy("ticket_id").orderBy("ticket_csat", "id").rowsBetween(-6, Window.currentRow)
w_all_by_event_at_moving_6 = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_by_ticket_csat_moving = Window.partitionBy("actor").orderBy(F.col("ticket_csat").desc(), "id").rowsBetween(-3, Window.currentRow)
w_ticket_id_by_ticket_csat_ful = Window.partitionBy("ticket_id").orderBy("ticket_csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_ticket_csat_sum = Window.partitionBy("ticket_id").orderBy("ticket_csat_sum_2_prior", "id")
w_ticket_id = Window.partitionBy("ticket_id")
w_last_ticket_csat_ticket_csat = Window.partitionBy("last_ticket_csat", "ticket_csat_change").orderBy("minutes_since_prev_event_at", "id")
w_ticket_id_by_ticket_csat_sum_2 = Window.partitionBy("ticket_id").orderBy("ticket_csat_sum_2_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_minutes_since_prev_ev = Window.orderBy("minutes_since_prev_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_csat_sum_2_prior_terc = Window.partitionBy("ticket_csat_sum_2_prior_tercile").orderBy(F.col("total_ticket_csat_avg_2").desc_nulls_first(), "ticket_csat_sum_2_prior_tercile", "first_ticket_csat", "ticket_csat_rank")
w_ticket_csat_sum_2_prior_terc_2 = Window.partitionBy("ticket_csat_sum_2_prior_tercile").orderBy(F.col("total_ticket_csat_avg_2").desc_nulls_first(), "ticket_csat_sum_2_prior_tercile", "first_ticket_csat", "ticket_csat_rank").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_csat_sum_2_prior_terc_3 = Window.partitionBy("ticket_csat_sum_2_prior_tercile").orderBy(F.col("total_ticket_csat_avg_2").desc_nulls_first(), "ticket_csat_sum_2_prior_tercile", "first_ticket_csat", "ticket_csat_rank").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_ticket_events
    .filter(~F.col("event_type").isin("assigned", "escalated"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_details_ticket = (
    valid.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.*",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

sequenced = (
    with_details_ticket
    .select(
        "id",
        "ticket_id",
        "actor",
        "ticket_tags",
        F.round(F.avg("ticket_csat").over(w_all_by_event_at_moving_1), 2).alias("ticket_csat_avg_2"),
        F.sum("ticket_csat").over(w_all_by_event_at_prior_2).alias("ticket_csat_sum_2_prior"),
        F.round(F.col("ticket_csat") / F.nullif(F.sum("ticket_csat").over(w_actor_ticket_id), F.lit(0)), 4).alias("ticket_csat_share_of_actor_ticket_id"),
        F.round(F.col("ticket_csat") / F.nullif(F.sum("ticket_csat").over(w_ticket_csat_ticket_id), F.lit(0)), 4).alias("ticket_csat_share_of_ticket_csat_ticket_id"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_by_ticket_csat))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.sum("ticket_csat").over(w_actor_ticket_id).alias("actor_ticket_id_sum_ticket_csat"),
        (F.col("ticket_csat") - F.lag("ticket_csat").over(w_all_by_event_at)).alias("ticket_csat_change"),
        F.max("ticket_csat").over(w_ticket_id_by_ticket_csat_mov).alias("ticket_csat_max_7"),
        F.round(F.avg("ticket_csat").over(w_all_by_event_at_moving_6), 2).alias("ticket_csat_avg_7"),
        F.rank().over(w_actor_by_ticket_csat).alias("ticket_csat_rank"),
        F.first("ticket_csat").over(w_all_by_event_at_full).alias("first_ticket_csat"),
        F.round(F.avg("ticket_csat").over(w_actor_by_ticket_csat_moving), 2).alias("ticket_csat_avg_4"),
        F.last("ticket_csat").over(w_ticket_id_by_ticket_csat_ful).alias("last_ticket_csat"),
    )
)

leaders = sequenced.filter(F.col("ticket_csat_rank") <= 2)

windowed = (
    leaders
    .select(
        "*",
        F.ntile(3).over(w_ticket_id_by_ticket_csat_sum).alias("ticket_csat_sum_2_prior_tercile"),
        F.round(F.col("ticket_csat_change") - F.avg("ticket_csat_change").over(w_ticket_id), 2).alias("ticket_csat_change_vs_ticket_id_avg"),
        F.row_number().over(w_last_ticket_csat_ticket_csat).alias("seq_no"),
        F.last("ticket_csat_max_7", ignorenulls=True).over(w_ticket_id_by_ticket_csat_sum_2).alias("last_known_ticket_csat_max_7"),
        F.first("minutes_since_prev_event_at", ignorenulls=True).over(w_all_by_minutes_since_prev_ev).alias("first_known_minutes_since_prev_event_at"),
    )
)

per_group = (
    windowed
    .groupBy(
        "ticket_csat_sum_2_prior_tercile",
        "first_ticket_csat",
        "ticket_csat_rank",
    )
    .agg(
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.count("*").alias("cnt"),
        F.countDistinct("actor").alias("distinct_actors"),
        F.sum("ticket_csat_avg_2").alias("total_ticket_csat_avg_2"),
    )
    .filter(F.col("cnt") >= 2)
)

with_trends = (
    per_group
    .select(
        "ticket_csat_sum_2_prior_tercile",
        "first_ticket_csat",
        "ticket_csat_rank",
        "actor_list",
        "cnt",
        "total_ticket_csat_avg_2",
        F.lead("first_ticket_csat").over(w_ticket_csat_sum_2_prior_terc).alias("next_first_ticket_csat"),
        F.last("distinct_actors").over(w_ticket_csat_sum_2_prior_terc_2).alias("last_distinct_actors"),
        F.min("total_ticket_csat_avg_2").over(w_ticket_csat_sum_2_prior_terc_3).alias("min_total_ticket_csat_avg_2_so_far"),
    )
)

result = (
    with_trends
    .filter(F.col("ticket_csat_sum_2_prior_tercile") > 5)
    .select(
        "ticket_csat_sum_2_prior_tercile",
        "first_ticket_csat",
        "ticket_csat_rank",
        "cnt",
        "total_ticket_csat_avg_2",
        "next_first_ticket_csat",
        "last_distinct_actors",
        F.least(F.col("total_ticket_csat_avg_2"), F.col("next_first_ticket_csat")).alias("least_total_ticket_csat_avg_2_next_first_ticket_"),
    )
    .orderBy("ticket_csat_sum_2_prior_tercile")
)
