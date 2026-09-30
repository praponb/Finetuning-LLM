from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_year_event_type_by = Window.partitionBy(F.year("event_at"), "event_type").orderBy("event_at", "id")
w_event_at_year_event_type_by_2 = Window.partitionBy(F.year("event_at"), "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_actor_len_running = Window.orderBy("actor_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_actor_len_by_last = Window.partitionBy("event_type", "actor_len").orderBy("last_known_avg_days_since_event_at", "id")
w_event_type_actor_len_by_last_2 = Window.partitionBy("event_type", "actor_len").orderBy("last_known_avg_days_since_event_at")
w_actor_len_by_avg_days_since = Window.partitionBy("actor_len").orderBy("avg_days_since_event_at")
w_event_type_actor_by_last_kno = Window.partitionBy("event_type", "actor").orderBy("last_known_actor_len", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_last_known_avg_days_since_ev = Window.partitionBy("last_known_avg_days_since_event_at_rank")

filtered = (
    df_ticket_events
    .filter((F.col("event_type") == "reopened") & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.when(F.col("actor").isin("agent", "customer"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
    )
)

enriched = (
    filtered
    .filter(F.col("ticket_id") <= 55)
    .withColumn("days_since_event_at", F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")))
)

actor_stats = (
    enriched
    .groupBy("actor")
    .agg(F.avg("days_since_event_at").alias("avg_days_since_event_at"))
)

above_average = (
    enriched.alias("s")
    .join(actor_stats.alias("g"), F.col("g.actor") == F.col("s.actor"), "inner")
    .filter(F.col("s.days_since_event_at") > F.col("g.avg_days_since_event_at"))
    .select(
        "s.*",
        "g.avg_days_since_event_at",
        F.round(F.col("s.days_since_event_at") - F.col("g.avg_days_since_event_at"), 2).alias("days_since_event_at_minus_avg"),
    )
)

with_windows = (
    above_average
    .select(
        "id",
        "event_type",
        "actor",
        "actor_len",
        "actor_group",
        "event_type_prefix",
        "avg_days_since_event_at",
        "days_since_event_at_minus_avg",
        F.round(F.percent_rank().over(w_event_at_year_event_type_by), 3).alias("event_at_percent_rank"),
        F.last("actor_len", ignorenulls=True).over(w_event_at_year_event_type_by_2).alias("last_known_actor_len"),
        F.last("avg_days_since_event_at", ignorenulls=True).over(w_all_by_actor_len_running).alias("last_known_avg_days_since_event_at"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "id",
        "event_type",
        "actor_group",
        "event_type_prefix",
        "avg_days_since_event_at",
        "days_since_event_at_minus_avg",
        "event_at_percent_rank",
        F.round(F.cume_dist().over(w_event_type_actor_len_by_last), 3).alias("last_known_avg_days_since_event_at_cume_dist"),
        F.rank().over(w_event_type_actor_len_by_last_2).alias("last_known_avg_days_since_event_at_rank"),
        F.rank().over(w_actor_len_by_avg_days_since).alias("avg_days_since_event_at_rank"),
        F.last("last_known_actor_len").over(w_event_type_actor_by_last_kno).alias("last_last_known_actor_len"),
    )
)

sequenced = (
    with_windows_2
    .select(
        "id",
        "actor_group",
        "event_at_percent_rank",
        "avg_days_since_event_at_rank",
        F.size(F.collect_set("actor_group").over(w_last_known_avg_days_since_ev)).alias("distinct_actor_group_in_last_known_avg_days_sinc"),
        F.count(F.lit(1)).over(w_last_known_avg_days_since_ev).alias("rows_in_last_known_avg_days_since_event_at_rank"),
        F.round(F.col("avg_days_since_event_at") / F.nullif(F.sum("avg_days_since_event_at").over(w_last_known_avg_days_since_ev), F.lit(0)), 4).alias("avg_days_since_event_at_share_of_last_known_avg_"),
    )
)

result = (
    sequenced
    .filter(F.col("distinct_actor_group_in_last_known_avg_days_sinc").between(2, 6))
    .select(
        "id",
        "actor_group",
        "event_at_percent_rank",
        "rows_in_last_known_avg_days_since_event_at_rank",
        "avg_days_since_event_at_share_of_last_known_avg_",
        F.when(F.col("actor_group").isin("group_a", "other"), "group_a").when(F.col("actor_group") == "group_b", "group_b").otherwise("other").alias("actor_group_group"),
    )
    .orderBy(F.col("actor_group_group").desc(), F.col("id").desc())
)
