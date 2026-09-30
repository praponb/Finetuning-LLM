from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at_runni = Window.partitionBy("event_type").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_to_date_by_event_at = Window.partitionBy(F.to_date("event_at")).orderBy(F.col("event_at").desc())
w_event_at_to_date_by_event_at_2 = Window.partitionBy(F.to_date("event_at")).orderBy(F.col("event_at").desc(), "id")
w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_at").desc(), "id")
w_event_type_by_event_at_full = Window.partitionBy("event_type").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_to_date_by_event_at_3 = Window.partitionBy(F.to_date("event_at")).orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_rn_by_event_at_quartile = Window.partitionBy("rn").orderBy("event_at_quartile", "id")
w_event_type = Window.partitionBy("event_type")
w_all_by_last_known_event_at_t = Window.orderBy("last_known_event_at")
w_rn_by_event_at_quartile_prio = Window.partitionBy("rn").orderBy("event_at_quartile", "id").rowsBetween(-3, -1)
w_actor_rn_by_event_at_percent = Window.partitionBy("actor", "rn").orderBy(F.col("event_at_percent_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_events = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

sequenced = (
    prepared_events
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        F.first("event_at", ignorenulls=True).over(w_event_type_by_event_at_runni).alias("first_known_event_at"),
        F.round(F.percent_rank().over(w_event_at_to_date_by_event_at), 3).alias("event_at_percent_rank"),
        F.lag("event_at").over(w_event_at_to_date_by_event_at_2).alias("prev_event_at"),
        F.ntile(4).over(w_actor_ticket_id_by_event_at).alias("event_at_quartile"),
        F.nth_value("event_at", 2).over(w_event_type_by_event_at_full).alias("second_event_at"),
        F.row_number().over(w_actor_ticket_id_by_event_at).alias("rn"),
        F.ntile(5).over(w_actor_ticket_id_by_event_at).alias("event_at_quintile"),
        F.last("event_at", ignorenulls=True).over(w_event_at_to_date_by_event_at_3).alias("last_known_event_at"),
        F.lead("event_at").over(w_event_type_by_event_at).alias("next_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_at_to_date_by_event_at_2))) / 60, 1).alias("minutes_since_prev_event_at"),
    )
)

ranked = (
    sequenced
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "first_known_event_at",
        "event_at_percent_rank",
        "event_at_quartile",
        "second_event_at",
        "event_at_quintile",
        "next_event_at",
        "minutes_since_prev_event_at",
        (F.col("event_at_quartile") - F.lag("event_at_quartile").over(w_rn_by_event_at_quartile)).alias("event_at_quartile_change"),
        F.round(F.col("rn") - F.avg("rn").over(w_event_type), 2).alias("rn_vs_event_type_avg"),
        F.dense_rank().over(w_all_by_last_known_event_at_t).alias("last_known_event_at_dense_rank"),
        F.max("event_at_quintile").over(w_rn_by_event_at_quartile_prio).alias("event_at_quintile_max_3_prior"),
        F.round(F.avg("event_at_quartile").over(w_event_type), 2).alias("event_type_avg_event_at_quartile"),
        F.nth_value("event_at_percent_rank", 2).over(w_actor_rn_by_event_at_percent).alias("second_event_at_percent_rank"),
        F.round(F.col("minutes_since_prev_event_at") - F.avg("minutes_since_prev_event_at").over(w_event_type), 2).alias("minutes_since_prev_event_at_vs_event_type_avg"),
    )
)

result = (
    ranked
    .select(
        "id",
        "event_type",
        "actor",
        "first_known_event_at",
        "event_at_quartile",
        "second_event_at",
        "minutes_since_prev_event_at",
        "event_at_quartile_change",
        "rn_vs_event_type_avg",
        "last_known_event_at_dense_rank",
        "event_at_quintile_max_3_prior",
        "event_type_avg_event_at_quartile",
        "second_event_at_percent_rank",
        "minutes_since_prev_event_at_vs_event_type_avg",
        F.round(F.col("rn_vs_event_type_avg") * 1.5, 2).alias("rn_vs_event_type_avg_adj"),
        F.coalesce(F.col("event_at_percent_rank"), F.lit(0.0)).alias("event_at_percent_rank_filled"),
    )
    .orderBy("actor")
)
