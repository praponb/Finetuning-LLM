from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy("event_at", "id")
w_all_by_event_at_ties = Window.orderBy("event_at")
w_event_at_rank = Window.partitionBy("event_at_rank")

prepared = (
    df_ticket_events
    .filter((F.col("actor") != "agent") & (F.col("event_type") == "reopened"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix"),
    )
)

derived = (
    prepared
    .select(
        "*",
        F.when(F.col("actor").isin("system", "customer"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group"),
        F.when(F.col("actor").isin("agent", "customer"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group_2"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
    )
)

with_trends = (
    derived
    .select(
        "*",
        F.dense_rank().over(w_all_by_event_at).alias("event_at_dense_rank"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.ntile(3).over(w_all_by_event_at).alias("event_at_tercile"),
        F.rank().over(w_all_by_event_at_ties).alias("event_at_rank"),
    )
)

scored = (
    with_trends
    .select(
        "id",
        "event_at",
        "actor",
        "actor_prefix",
        "actor_group",
        "actor_group_2",
        "event_at_label",
        "event_at_tercile",
        "event_at_rank",
        F.substring(F.trim(F.col("actor")), 1, 3).alias("actor_prefix_2"),
    )
)

with_trends_2 = (
    scored
    .withColumn("event_at_rank_vs_event_at_rank_avg", F.round(F.col("event_at_rank") - F.avg("event_at_rank").over(w_event_at_rank), 2))
    .withColumn("event_at_percent_rank", F.round(F.percent_rank().over(w_all_by_event_at_ties), 3))
)

result = (
    with_trends_2
    .groupingSets(
        [["actor", "event_at_tercile", "actor_group_2"], ["actor"], ["event_at_tercile"]],
        "actor", "event_at_tercile", "actor_group_2",
    )
    .agg(F.array_sort(F.collect_set("actor")).alias("actor_list"))
    .orderBy(
        F.col("actor").asc_nulls_last(),
        F.col("event_at_tercile").asc_nulls_last(),
        F.col("actor_group_2").asc_nulls_last(),
    )
)
