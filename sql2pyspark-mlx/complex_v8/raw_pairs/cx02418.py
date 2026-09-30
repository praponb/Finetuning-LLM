from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_year = Window.partitionBy("event_at_year")
w_event_event_type_n_event_eve = Window.partitionBy("event_event_type", "n_event_event_type_in_resolved").orderBy("n_event_event_type_in_assigned", "event_at_year", "event_event_type")
w_last_event_at_month = Window.partitionBy(F.date_trunc("month", "last_event_at"))
w_n_event_event_type_in_assign = Window.partitionBy("n_event_event_type_in_assigned").orderBy("n_event_event_type_in_resolved", "event_at_year", "event_event_type").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_event_event_type_in_assign_2 = Window.partitionBy("n_event_event_type_in_assigned").orderBy("n_event_event_type_in_resolved")
w_n_event_event_type_in_assign_3 = Window.partitionBy("n_event_event_type_in_assigned").orderBy("n_event_event_type_in_resolved", "event_at_year", "event_event_type")

scoped = (
    df_ticket_events
    .filter((F.month(F.col("event_at")).isin(1, 3)) | (F.col("event_type").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("actor").isin("system", "agent"), "group_a").when(F.col("actor") == "customer", "group_b").otherwise("other").alias("actor_group"),
        (F.col("event_at") + F.expr("INTERVAL 30 MINUTES")).alias("event_at_plus_30m"),
        F.concat_ws(": ", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

with_children = (
    scoped.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_at_plus_30m",
        "s.event_type_actor_label",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

totals = (
    with_children
    .groupBy(F.year("event_at").alias("event_at_year"), "event_event_type")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.sum(F.when(F.col("event_event_type").isin("assigned", "created", "resolved"), 1).otherwise(0)).alias("n_event_event_type_in_assigned"),
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("event_actor")).alias("event_actor_list"),
        F.array_sort(F.collect_set("event_event_type")).alias("event_event_type_list"),
        F.count(F.when(F.col("event_event_type").isin("resolved", "created"), 1)).alias("n_event_event_type_in_resolved"),
    )
    .filter(F.col("cnt") >= 2)
)

ranked = (
    totals
    .select(
        "*",
        F.round(F.avg("event_at_year").over(w_event_at_year), 2).alias("event_at_year_avg_event_at_year"),
        F.rank().over(w_event_event_type_n_event_eve).alias("n_event_event_type_in_assigned_rank"),
    )
)

tagged = (
    ranked
    .filter(F.col("event_event_type").isin("resolved", "escalated", "replied"))
    .select(
        "*",
        (F.col("event_at_year") - F.col("cnt")).alias("event_at_year_minus_cnt"),
        F.greatest(F.col("event_at_year"), F.col("cnt")).alias("greatest_event_at_year_cnt"),
    )
)

ranked_2 = (
    tagged
    .select(
        "event_at_year",
        "event_event_type",
        "n_event_event_type_in_assigned",
        "cnt",
        "event_event_type_list",
        "n_event_event_type_in_resolved",
        "event_at_year_avg_event_at_year",
        F.round(F.col("event_at_year") - F.avg("event_at_year").over(w_last_event_at_month), 2).alias("event_at_year_vs_last_event_at_month_avg"),
        F.nth_value("event_at_year_minus_cnt", 2).over(w_n_event_event_type_in_assign).alias("second_event_at_year_minus_cnt"),
        F.round(F.percent_rank().over(w_n_event_event_type_in_assign_2), 3).alias("n_event_event_type_in_resolved_percent_rank"),
        F.size(F.collect_set("event_event_type").over(w_last_event_at_month)).alias("distinct_event_event_type_in_last_event_at_month"),
        F.lead("greatest_event_at_year_cnt").over(w_n_event_event_type_in_assign_3).alias("next_greatest_event_at_year_cnt"),
        F.round(F.col("greatest_event_at_year_cnt") - F.avg("greatest_event_at_year_cnt").over(w_last_event_at_month), 2).alias("greatest_event_at_year_cnt_vs_last_event_at_mont"),
    )
)

result = (
    ranked_2
    .select(
        "event_at_year",
        "event_event_type",
        "event_event_type_list",
        "n_event_event_type_in_resolved",
        "event_at_year_avg_event_at_year",
        "event_at_year_vs_last_event_at_month_avg",
        "n_event_event_type_in_resolved_percent_rank",
        "distinct_event_event_type_in_last_event_at_month",
        "next_greatest_event_at_year_cnt",
        "greatest_event_at_year_cnt_vs_last_event_at_mont",
        F.substring(F.trim(F.col("event_event_type")), 1, 2).alias("event_event_type_prefix"),
        F.coalesce(F.col("event_at_year"), F.lit(0)).alias("event_at_year_filled"),
    )
    .orderBy(F.col("n_event_event_type_in_resolved").desc_nulls_first())
)
