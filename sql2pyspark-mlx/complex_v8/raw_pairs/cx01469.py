from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat = Window.partitionBy("csat")
w_priority_by_greatest_tags_co = Window.partitionBy("priority").orderBy("greatest_tags_count_requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_by_greatest_tags_co_2 = Window.partitionBy("priority").orderBy("greatest_tags_count_requester_id")
w_priority_by_greatest_tags_co_3 = Window.partitionBy("priority").orderBy("greatest_tags_count_requester_id", "id")
w_priority_by_greatest_tags_co_4 = Window.partitionBy("priority").orderBy("greatest_tags_count_requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_csat_change_by_csat = Window.partitionBy("priority", "csat_change").orderBy(F.col("csat_vs_csat_avg").desc(), "id", "tag").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tag_first_known_tags_count = Window.partitionBy("tag", "first_known_tags_count")

recent = (
    df_tickets
    .filter(F.col("opened_at") < F.lit("2025-02-18 11:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.size(F.col("tags")).alias("tags_count"),
        F.array_contains(F.col("tags"), "outage").alias("has_outage"),
        F.greatest(F.col("requester_id"), F.col("csat")).alias("greatest_requester_id_csat"),
    )
)

joined = (
    recent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

tagged = (
    joined
    .withColumn("greatest_tags_count_requester_id", F.greatest(F.col("tags_count"), F.col("requester_id")))
)

with_windows = (
    tagged
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        "tags_count",
        "has_outage",
        "greatest_requester_id_csat",
        "agent_tier",
        "greatest_tags_count_requester_id",
        F.round(F.col("csat") - F.avg("csat").over(w_csat), 2).alias("csat_vs_csat_avg"),
        F.first("tags_count", ignorenulls=True).over(w_priority_by_greatest_tags_co).alias("first_known_tags_count"),
        F.round(F.percent_rank().over(w_priority_by_greatest_tags_co_2), 3).alias("greatest_tags_count_requester_id_percent_rank"),
        (F.col("csat") - F.lag("csat").over(w_priority_by_greatest_tags_co_3)).alias("csat_change"),
        F.nth_value("closed_at", 2).over(w_priority_by_greatest_tags_co_4).alias("second_closed_at"),
    )
)

exploded = (
    with_windows
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags_count",
        "has_outage",
        "greatest_requester_id_csat",
        "agent_tier",
        "greatest_tags_count_requester_id",
        "csat_vs_csat_avg",
        "first_known_tags_count",
        "greatest_tags_count_requester_id_percent_rank",
        "csat_change",
        "second_closed_at",
        F.explode("tags").alias("tag"),
    )
)

sequenced = (
    exploded
    .select(
        "id",
        "requester_id",
        "priority",
        "tags_count",
        "has_outage",
        "greatest_requester_id_csat",
        "csat_vs_csat_avg",
        "first_known_tags_count",
        "csat_change",
        "second_closed_at",
        "tag",
        F.nth_value("greatest_requester_id_csat", 2).over(w_priority_csat_change_by_csat).alias("second_greatest_requester_id_csat"),
        F.size(F.collect_set("agent_id").over(w_tag_first_known_tags_count)).alias("distinct_agent_id_in_tag_first_known_tags_count"),
    )
)

result = (
    sequenced
    .filter(F.col("greatest_requester_id_csat").between(3, 19))
    .select(
        "id",
        "requester_id",
        "priority",
        "has_outage",
        "greatest_requester_id_csat",
        "first_known_tags_count",
        "csat_change",
        "second_closed_at",
        "tag",
        "distinct_agent_id_in_tag_first_known_tags_count",
    )
    .orderBy(
        "first_known_tags_count",
        F.col("distinct_agent_id_in_tag_first_known_tags_count").asc(),
    )
)
