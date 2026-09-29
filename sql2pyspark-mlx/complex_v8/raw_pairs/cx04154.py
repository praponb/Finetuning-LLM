from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_event_at_year_by_e = Window.partitionBy("ticket_id", F.year("event_at")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_by_event_at = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_by_event_at_full = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_event_at_year_by_e_2 = Window.partitionBy("ticket_id", F.year("event_at")).orderBy("event_at")
w_ticket_id_event_at_year_by_e_3 = Window.partitionBy("ticket_id", F.year("event_at")).orderBy("event_at", "id")
w_event_type_event_at_year_by = Window.partitionBy("event_type", F.year("event_at")).orderBy(F.col("event_at").desc(), "id")
w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").asc_nulls_last())
w_ticket_id_actor_by_event_at_2 = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_last_event_at_mont = Window.partitionBy("ticket_id", F.month("last_event_at"))
w_next2_event_at_to_date = Window.partitionBy(F.to_date("next2_event_at"))
w_all_by_next2_event_at_moving = Window.orderBy("next2_event_at", "id").rowsBetween(-2, Window.currentRow)
w_ticket_id_by_event_at_dense = Window.partitionBy("ticket_id").orderBy(F.col("event_at_dense_rank_2").desc(), "id")

recent = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.hour(F.col("event_at")).between(8, 12)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_windows = (
    recent
    .select(
        "*",
        F.first("event_at").over(w_ticket_id_event_at_year_by_e).alias("first_event_at"),
        F.dense_rank().over(w_actor_by_event_at).alias("event_at_dense_rank"),
        F.last("event_at").over(w_actor_by_event_at_full).alias("last_event_at"),
        F.dense_rank().over(w_ticket_id_event_at_year_by_e_2).alias("event_at_dense_rank_2"),
        F.ntile(4).over(w_ticket_id_event_at_year_by_e_3).alias("event_at_quartile"),
        F.lead("event_at", 2).over(w_ticket_id_event_at_year_by_e_3).alias("next2_event_at"),
        F.lag("event_at", 3).over(w_event_type_event_at_year_by).alias("prev3_event_at"),
        F.round(F.cume_dist().over(w_ticket_id_actor_by_event_at), 3).alias("event_at_cume_dist"),
        F.nth_value("event_at", 2).over(w_ticket_id_actor_by_event_at_2).alias("second_event_at"),
        F.lag("event_at").over(w_ticket_id_event_at_year_by_e_3).alias("prev_event_at"),
    )
)

top_rows = with_windows.filter(F.col("event_at_dense_rank") <= 5)

metrics = (
    top_rows
    .select(
        "*",
        F.round(F.col("event_at_cume_dist") / F.nullif(F.sum("event_at_cume_dist").over(w_ticket_id_last_event_at_mont), F.lit(0)), 4).alias("event_at_cume_dist_share_of_ticket_id_last_event"),
        F.sum("event_at_dense_rank_2").over(w_next2_event_at_to_date).alias("next2_event_at_to_date_sum_event_at_dense_rank_2"),
        F.round(F.avg("event_at_quartile").over(w_all_by_next2_event_at_moving), 2).alias("event_at_quartile_avg_3"),
        F.ntile(4).over(w_ticket_id_by_event_at_dense).alias("event_at_dense_rank_2_quartile"),
        F.count(F.lit(1)).over(w_next2_event_at_to_date).alias("rows_in_next2_event_at_to_date"),
        F.round(F.col("event_at_quartile") - F.avg("event_at_quartile").over(w_next2_event_at_to_date), 2).alias("event_at_quartile_vs_next2_event_at_to_date_avg"),
        F.size(F.collect_set("actor").over(w_ticket_id_last_event_at_mont)).alias("distinct_actor_in_ticket_id_last_event_at_month"),
        F.count(F.lit(1)).over(w_ticket_id_last_event_at_mont).alias("rows_in_ticket_id_last_event_at_month"),
    )
)

result = (
    metrics
    .filter(F.col("actor") == "agent")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "first_event_at",
        "last_event_at",
        "event_at_dense_rank_2",
        "event_at_quartile",
        "prev3_event_at",
        "event_at_cume_dist",
        "second_event_at",
        "prev_event_at",
        "next2_event_at_to_date_sum_event_at_dense_rank_2",
        "event_at_quartile_avg_3",
        "event_at_quartile_vs_next2_event_at_to_date_avg",
        "rows_in_ticket_id_last_event_at_month",
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
    .orderBy(F.col("event_type").desc(), "id")
    .limit(5)
)
