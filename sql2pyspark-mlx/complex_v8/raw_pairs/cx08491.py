from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_running = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy("event_at")
w_actor_ticket_id_by_event_at_2 = Window.partitionBy("actor", "ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_second_event_at_month_by_eve = Window.partitionBy(F.month("second_event_at")).orderBy(F.col("event_at_rank").desc())
w_second_event_at_month_by_eve_2 = Window.partitionBy(F.month("second_event_at")).orderBy(F.col("event_at_rank").desc(), "id")
w_second_event_at_month = Window.partitionBy(F.month("second_event_at"))

base = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 18)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("actor")), 1, 4).alias("actor_prefix"),
    )
)

ranked = (
    base
    .select(
        "id",
        "actor",
        "actor_prefix",
        F.rank().over(w_all_by_event_at).alias("event_at_rank"),
        F.last("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("last_known_event_at"),
        F.round(F.cume_dist().over(w_actor_ticket_id_by_event_at), 3).alias("event_at_cume_dist"),
        F.nth_value("event_at", 2).over(w_actor_ticket_id_by_event_at_2).alias("second_event_at"),
    )
)

ranked_2 = (
    ranked
    .select(
        "id",
        "actor",
        "actor_prefix",
        "last_known_event_at",
        "event_at_cume_dist",
        F.round(F.percent_rank().over(w_second_event_at_month_by_eve), 3).alias("event_at_rank_percent_rank"),
        (F.col("event_at_rank") - F.lag("event_at_rank").over(w_second_event_at_month_by_eve_2)).alias("event_at_rank_change"),
        F.dense_rank().over(w_second_event_at_month_by_eve).alias("event_at_rank_dense_rank"),
        F.round(F.col("event_at_cume_dist") - F.avg("event_at_cume_dist").over(w_second_event_at_month), 2).alias("event_at_cume_dist_vs_second_event_at_month_avg"),
    )
)

screened = (
    ranked_2
    .crossJoin(ranked_2.agg(F.min("event_at_rank_change").alias("overall_min_event_at_rank_change")))
    .filter(F.col("event_at_rank_change") > F.col("overall_min_event_at_rank_change"))
    .drop("overall_min_event_at_rank_change")
)

result = (
    screened
    .cube("event_at_rank_dense_rank", "actor")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("actor").alias("distinct_actors"),
    )
)
