from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy("event_at")
w_actor_ticket_id_by_event_at_2 = Window.partitionBy("actor", "ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_ticket_id_by_event_at_3 = Window.partitionBy("actor", "ticket_id").orderBy("event_at", "id")

filtered = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("event_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("actor")), 1, 4).alias("actor_prefix"),
    )
)

sequenced = (
    filtered
    .select(
        "*",
        F.round(F.cume_dist().over(w_actor_ticket_id_by_event_at), 3).alias("event_at_cume_dist"),
        F.nth_value("event_at", 2).over(w_actor_ticket_id_by_event_at_2).alias("second_event_at"),
        F.row_number().over(w_actor_ticket_id_by_event_at_3).alias("event_at_seq"),
        F.round(F.percent_rank().over(w_actor_ticket_id_by_event_at), 3).alias("event_at_percent_rank"),
    )
)

derived = (
    sequenced
    .filter(F.hour(F.col("second_event_at")).between(8, 11))
    .select(
        "*",
        F.round(F.col("event_at_cume_dist") * 1.1, 2).alias("event_at_cume_dist_adj"),
        (F.col("event_at_seq") + F.col("event_at_percent_rank")).alias("event_at_seq_plus_event_at_percent_rank"),
        F.concat(F.col("actor_prefix"), F.lit("#"), F.col("id").cast("string")).alias("actor_prefix_ref"),
    )
)

result = (
    derived
    .cube("event_type", "actor", "ticket_id")
    .agg(
        F.round(F.sum("event_at_cume_dist") / F.nullif(F.sum("event_at_seq_plus_event_at_percent_rank"), F.lit(0)), 3).alias("event_at_cume_dist_to_event_at_seq_plus_event_at"),
        F.percentile_approx("event_at_percent_rank", 0.5).alias("median_event_at_percent_rank"),
        F.countDistinct("event_type").alias("distinct_event_types"),
    )
    .orderBy(
        F.col("event_type").asc_nulls_last(),
        F.col("actor").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
    )
)
