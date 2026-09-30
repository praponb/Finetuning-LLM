from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_hour = Window.partitionBy("event_at_hour")
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_all_by_event_at_prior_1 = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(-1, -1)

recent_events = (
    df_ticket_events
    .filter(F.hour(F.col("event_at")).between(8, 12))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        (F.col("event_at") + F.expr("INTERVAL 15 MINUTES")).alias("event_at_plus_15m"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.hour(F.col("event_at")).alias("event_at_hour"),
    )
)

with_trends = (
    recent_events
    .select(
        "*",
        F.sum("event_at_hour").over(w_event_at_hour).alias("event_at_hour_sum_event_at_hour"),
        F.first("event_at").over(w_all_by_event_at_full).alias("first_event_at"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.round(F.col("event_at_hour") / F.nullif(F.sum("event_at_hour").over(w_event_at_hour), F.lit(0)), 4).alias("event_at_hour_share_of_event_at_hour"),
        F.count(F.lit(1)).over(w_event_at_hour).alias("rows_in_event_at_hour"),
        F.round(F.avg("event_at_hour").over(w_all_by_event_at_prior_1), 2).alias("event_at_hour_avg_1_prior"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["event_at_hour", "ticket_id"], ["event_at_hour"], ["ticket_id"], []],
        "event_at_hour", "ticket_id",
    )
    .agg(
        F.round(100 * F.count(F.when(~F.col("event_type").isin("replied", "assigned"), 1)) / F.count("*"), 1).alias("pct_event_type_not_in_replied_as"),
        F.max("first_event_at").alias("last_first_event_at"),
    )
)
