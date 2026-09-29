from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_event_at_ties = Window.partitionBy("actor").orderBy("event_at")
w_event_at_year_by_event_at_ti = Window.partitionBy(F.year("event_at")).orderBy("event_at")
w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy("event_at")
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_event_type_or_unknown_by_eve = Window.partitionBy("event_type_or_unknown").orderBy("event_at", "id")
w_event_at_to_date_event_type = Window.partitionBy(F.to_date("event_at"), "event_type").orderBy("event_at", "id")
w_actor_event_at_dense_rank_by = Window.partitionBy("actor", "event_at_dense_rank").orderBy(F.col("event_at_quintile").desc(), "id")
w_event_type_event_at_dense_ra = Window.partitionBy("event_type", "event_at_dense_rank").orderBy(F.col("seq_no").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_quintile = Window.partitionBy("event_at_quintile")
w_actor_event_at_dense_rank_by_2 = Window.partitionBy("actor", "event_at_dense_rank").orderBy(F.col("event_at_quintile").desc())

prepared = (
    df_ticket_events
    .filter(F.col("event_type") == "assigned")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.upper(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

with_trends = (
    prepared
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_type_clean",
        F.rank().over(w_actor_by_event_at_ties).alias("event_at_rank"),
        F.dense_rank().over(w_actor_by_event_at_ties).alias("event_at_dense_rank"),
        F.round(F.cume_dist().over(w_event_at_year_by_event_at_ti), 3).alias("event_at_cume_dist"),
        F.rank().over(w_ticket_id_by_event_at_ties).alias("event_at_rank_2"),
        F.ntile(5).over(w_ticket_id_by_event_at).alias("event_at_quintile"),
        F.lead("event_at", 2).over(w_event_type_or_unknown_by_eve).alias("next2_event_at"),
        F.row_number().over(w_ticket_id_by_event_at).alias("seq_no"),
        F.round(F.cume_dist().over(w_event_at_to_date_event_type), 3).alias("event_at_cume_dist_2"),
        F.rank().over(w_event_at_to_date_event_type).alias("event_at_rank_3"),
        F.lead("event_at", 2).over(w_event_at_to_date_event_type).alias("next2_event_at_2"),
    )
)

leaders = with_trends.filter(F.col("event_at_rank_3") <= 2)

ranked = (
    leaders
    .select(
        "id",
        "event_at",
        "actor",
        "event_at_rank_2",
        "event_at_quintile",
        "event_at_cume_dist_2",
        "event_at_rank_3",
        F.round((F.unix_timestamp("next2_event_at_2") - F.unix_timestamp(F.lag("next2_event_at_2").over(w_actor_event_at_dense_rank_by))) / 60, 1).alias("minutes_since_prev_next2_event_at_2"),
        F.last("next2_event_at_2").over(w_event_type_event_at_dense_ra).alias("last_next2_event_at_2"),
        F.round(F.col("event_at_cume_dist_2") - F.avg("event_at_cume_dist_2").over(w_event_at_quintile), 2).alias("event_at_cume_dist_2_vs_event_at_quintile_avg"),
        F.first("event_at_rank_3").over(w_event_type_event_at_dense_ra).alias("first_event_at_rank_3"),
        F.max("event_at_rank_2").over(w_event_at_quintile).alias("event_at_quintile_max_event_at_rank_2"),
        F.max("event_at_cume_dist_2").over(w_event_at_quintile).alias("event_at_quintile_max_event_at_cume_dist_2"),
        F.size(F.collect_set("event_type").over(w_event_at_quintile)).alias("distinct_event_type_in_event_at_quintile"),
        F.rank().over(w_actor_event_at_dense_rank_by_2).alias("event_at_quintile_rank"),
    )
)

result = (
    ranked
    .filter(F.col("event_at_quintile") < 8)
    .select(
        "id",
        "event_at",
        "actor",
        "event_at_rank_2",
        "event_at_rank_3",
        "minutes_since_prev_next2_event_at_2",
        "last_next2_event_at_2",
        "event_at_cume_dist_2_vs_event_at_quintile_avg",
        "first_event_at_rank_3",
        "distinct_event_type_in_event_at_quintile",
        "event_at_quintile_rank",
    )
    .orderBy(F.col("event_at_rank_2").asc(), "id")
    .limit(5)
)
