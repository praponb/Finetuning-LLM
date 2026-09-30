from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at_full = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_or_unknown_ticket_id_b = Window.partitionBy("actor_or_unknown", "ticket_id").orderBy(F.col("event_at").desc(), "id")
w_actor_by_event_at_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_or_unknown_ticket_id_b_2 = Window.partitionBy("actor_or_unknown", "ticket_id").orderBy(F.col("event_at").desc())
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy("event_at")
w_actor_by_event_at = Window.partitionBy("actor").orderBy("event_at", "id")
w_event_type = Window.partitionBy("event_type")
w_actor_first_known_event_at_m = Window.partitionBy("actor", F.month("first_known_event_at")).orderBy("event_at_percent_rank", "id")
w_actor_first_known_event_at_m_2 = Window.partitionBy("actor", F.month("first_known_event_at")).orderBy("event_at_percent_rank")
w_event_at_quintile_2_by_event = Window.partitionBy("event_at_quintile_2").orderBy("event_at_quintile_2", "id")
w_actor_event_at_quintile_by_e = Window.partitionBy("actor", "event_at_quintile").orderBy("event_at_quartile", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_ticket_events
    .filter((F.to_date(F.col("event_at")) < F.lit("2025-02-06").cast("date")) & (F.col("ticket_id") <= 28))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

sequenced = (
    valid
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "actor_or_unknown",
        F.last("event_at").over(w_event_type_by_event_at_full).alias("last_event_at"),
        F.ntile(5).over(w_actor_or_unknown_ticket_id_b).alias("event_at_quintile"),
        F.last("event_at", ignorenulls=True).over(w_actor_by_event_at_running).alias("last_known_event_at"),
        F.first("event_at", ignorenulls=True).over(w_actor_by_event_at_running).alias("first_known_event_at"),
        F.round(F.cume_dist().over(w_actor_or_unknown_ticket_id_b_2), 3).alias("event_at_cume_dist"),
        F.ntile(4).over(w_event_type_by_event_at).alias("event_at_quartile"),
        F.round(F.percent_rank().over(w_event_type_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.row_number().over(w_event_type_by_event_at).alias("event_at_seq"),
        F.ntile(5).over(w_actor_by_event_at).alias("event_at_quintile_2"),
        F.rank().over(w_event_type_by_event_at).alias("event_at_rank"),
    )
)

with_trends = (
    sequenced
    .select(
        "*",
        F.round(F.col("event_at_quintile") - F.avg("event_at_quintile").over(w_event_type), 2).alias("event_at_quintile_vs_event_type_avg"),
        F.round(F.col("event_at_quintile_2") - F.avg("event_at_quintile_2").over(w_event_type), 2).alias("event_at_quintile_2_vs_event_type_avg"),
        F.lead("last_event_at").over(w_actor_first_known_event_at_m).alias("next_last_event_at"),
        F.round(F.percent_rank().over(w_actor_first_known_event_at_m_2), 3).alias("event_at_percent_rank_percent_rank"),
        F.row_number().over(w_event_at_quintile_2_by_event).alias("event_at_quintile_2_seq"),
        F.round(F.col("event_at_cume_dist") / F.nullif(F.sum("event_at_cume_dist").over(w_event_type), F.lit(0)), 4).alias("event_at_cume_dist_share_of_event_type"),
        F.first("first_known_event_at", ignorenulls=True).over(w_actor_event_at_quintile_by_e).alias("first_known_first_known_event_at"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "event_type",
        "actor",
        "actor_or_unknown",
        "event_at_quintile",
        "first_known_event_at",
        "event_at_quartile",
        "event_at_percent_rank",
        "event_at_seq",
        "event_at_quintile_vs_event_type_avg",
        "next_last_event_at",
        "event_at_percent_rank_percent_rank",
        "event_at_cume_dist_share_of_event_type",
        "first_known_first_known_event_at",
        F.abs(F.col("event_at_percent_rank") - 0.7).alias("event_at_percent_rank_distance"),
        F.coalesce(F.col("actor_or_unknown"), F.lit("UNKNOWN")).alias("actor_or_unknown_or_unknown"),
    )
    .orderBy(F.col("actor_or_unknown").asc_nulls_last())
)
