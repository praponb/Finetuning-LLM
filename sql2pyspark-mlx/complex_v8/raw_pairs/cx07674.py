from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_n_event_type_resolved = Window.orderBy(F.col("n_event_type_resolved").desc(), "event_at_month", "event_type", "actor_or_unknown")
w_actor_or_unknown_n_event_typ = Window.partitionBy("actor_or_unknown", "n_event_type_not_in_escalated")
w_actor_or_unknown_n_event_typ_2 = Window.partitionBy("actor_or_unknown", "n_event_type_assigned").orderBy("n_event_type_resolved", "event_at_month", "event_type", "actor_or_unknown")
w_actor_or_unknown_n_event_typ_3 = Window.partitionBy("actor_or_unknown", "n_event_type_assigned").orderBy("n_event_type_resolved", "event_at_month", "event_type", "actor_or_unknown").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_or_unknown_by_n_event = Window.partitionBy("actor_or_unknown").orderBy("n_event_type_resolved_tercile_2")
w_event_type = Window.partitionBy("event_type")

base = (
    df_ticket_events
    .filter(F.col("event_type") != "replied")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

summary = (
    base
    .filter(F.col("actor").isNotNull())
    .groupBy(
        F.date_trunc("month", "event_at").alias("event_at_month"),
        "event_type",
        "actor_or_unknown",
    )
    .agg(
        F.count(F.when(~F.col("event_type").isin("escalated", "assigned"), 1)).alias("n_event_type_not_in_escalated"),
        F.count(F.when(F.col("event_type") != "resolved", 1)).alias("n_event_type_resolved"),
        F.sum(F.when(F.col("event_type") == "assigned", 1).otherwise(0)).alias("n_event_type_assigned"),
        F.min("event_at").alias("first_event_at"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
    )
)

with_trends = (
    summary
    .select(
        "*",
        F.ntile(3).over(w_all_by_n_event_type_resolved).alias("n_event_type_resolved_tercile"),
        F.round(F.col("n_event_type_resolved") - F.avg("n_event_type_resolved").over(w_actor_or_unknown_n_event_typ), 2).alias("n_event_type_resolved_vs_actor_or_unknown_n_even"),
        F.ntile(3).over(w_actor_or_unknown_n_event_typ_2).alias("n_event_type_resolved_tercile_2"),
        F.nth_value("first_event_at", 2).over(w_actor_or_unknown_n_event_typ_3).alias("second_first_event_at"),
    )
)

filtered = (
    with_trends
    .crossJoin(with_trends.agg(F.avg("n_event_type_assigned").alias("overall_avg_n_event_type_assigned")))
    .filter(F.col("n_event_type_assigned") > F.col("overall_avg_n_event_type_assigned"))
    .drop("overall_avg_n_event_type_assigned")
)

metrics = (
    filtered
    .withColumn("n_event_type_resolved_tercile_2_percent_rank", F.round(F.percent_rank().over(w_actor_or_unknown_by_n_event), 3))
    .withColumn("n_event_type_resolved_vs_event_type_avg", F.round(F.col("n_event_type_resolved") - F.avg("n_event_type_resolved").over(w_event_type), 2))
)

result = (
    metrics
    .select(
        "event_at_month",
        "actor_or_unknown",
        "n_event_type_assigned",
        "event_type_list",
        "n_event_type_resolved_tercile",
        "n_event_type_resolved_vs_actor_or_unknown_n_even",
        "n_event_type_resolved_tercile_2",
        "second_first_event_at",
        "n_event_type_resolved_tercile_2_percent_rank",
        (F.col("first_event_at") + F.expr("INTERVAL 7 DAYS")).alias("first_event_at_plus_7d"),
        F.size(F.col("event_type_list")).alias("event_type_list_count"),
    )
    .orderBy(F.col("n_event_type_resolved_tercile").desc_nulls_first())
)
