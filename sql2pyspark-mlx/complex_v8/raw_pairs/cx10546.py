from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_channel_groups_medi = Window.partitionBy("distinct_channel_groups", "median_csat").orderBy(F.col("median_csat").asc_nulls_last(), "event_actor", "event_at_year")
w_event_actor_median_csat_by_d = Window.partitionBy("event_actor", "median_csat").orderBy(F.col("distinct_channel_groups").desc(), "event_actor", "event_at_year")
w_event_actor = Window.partitionBy("event_actor")
w_distinct_channel_groups_medi_2 = Window.partitionBy("distinct_channel_groups", "median_csat").orderBy(F.col("median_csat").asc_nulls_last(), "event_actor", "event_at_year").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_tickets
    .filter(~F.col("priority").isin("P1", "P2"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "channel",
        "csat",
        F.when(F.col("channel").isin("chat", "web"), "group_a").when(F.col("channel") == "phone", "group_b").otherwise("other").alias("channel_group"),
        F.dayofweek(F.col("opened_at")).alias("opened_at_dayofweek"),
        F.date_trunc("quarter", F.col("closed_at")).alias("closed_at_quarter"),
    )
)

line_level_events = (
    filtered.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.csat",
        "s.channel_group",
        "s.closed_at_quarter",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
    )
)

totals = (
    line_level_events
    .groupBy("event_actor", F.year("event_at").alias("event_at_year"))
    .agg(
        F.percentile_approx("csat", 0.5).alias("median_csat"),
        F.count("*").alias("n_rows"),
        F.countDistinct("channel_group").alias("distinct_channel_groups"),
    )
)

qualified = (
    totals
    .crossJoin(totals.agg(F.avg("median_csat").alias("overall_avg_median_csat")))
    .filter(F.col("median_csat") > F.col("overall_avg_median_csat"))
    .drop("overall_avg_median_csat")
)

metrics = (
    qualified
    .select(
        "*",
        F.round(F.percent_rank().over(w_distinct_channel_groups_medi), 3).alias("median_csat_percent_rank"),
        F.lead("distinct_channel_groups").over(w_event_actor_median_csat_by_d).alias("next_distinct_channel_groups"),
        (F.col("distinct_channel_groups") - F.lag("distinct_channel_groups").over(w_distinct_channel_groups_medi)).alias("distinct_channel_groups_change"),
        (F.col("event_at_year") - F.lag("event_at_year").over(w_distinct_channel_groups_medi)).alias("event_at_year_change"),
        F.max("median_csat").over(w_event_actor).alias("event_actor_max_median_csat"),
        F.nth_value("distinct_channel_groups", 2).over(w_distinct_channel_groups_medi_2).alias("second_distinct_channel_groups"),
    )
)

with_activity = (
    metrics
    .crossJoin(metrics.agg(F.min("event_at_year").alias("overall_min_event_at_year")))
    .filter(F.col("event_at_year") > F.col("overall_min_event_at_year"))
    .drop("overall_min_event_at_year")
)

result = (
    with_activity
    .select(
        "event_actor",
        "event_at_year",
        "median_csat",
        "n_rows",
        "distinct_channel_groups",
        "median_csat_percent_rank",
        "distinct_channel_groups_change",
        "event_actor_max_median_csat",
        "second_distinct_channel_groups",
        F.least(F.col("median_csat_percent_rank"), F.col("n_rows")).alias("least_median_csat_percent_rank_n_rows"),
    )
)
