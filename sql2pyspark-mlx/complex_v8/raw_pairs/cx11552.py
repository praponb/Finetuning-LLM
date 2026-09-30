from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_group_by_csat = Window.partitionBy("priority_group").orderBy(F.col("csat").asc_nulls_last(), "id")
w_priority_group_by_csat_full = Window.partitionBy("priority_group").orderBy(F.col("csat").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_tickets
    .filter(F.size(F.col("tags")) >= 2)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "channel",
        "csat",
        F.dayofweek(F.col("opened_at")).alias("opened_at_dayofweek"),
        F.when(F.col("priority").isin("P2", "P4"), "group_a").when(F.col("priority") == "P3", "group_b").otherwise("other").alias("priority_group"),
    )
)

metrics = (
    prepared
    .select(
        "*",
        F.lag("requester_id", 3, 0).over(w_priority_group_by_csat).alias("prev3_requester_id"),
        F.nth_value("requester_id", 2).over(w_priority_group_by_csat_full).alias("second_requester_id"),
        F.lag("requester_id", 1, 0).over(w_priority_group_by_csat).alias("prev_requester_id"),
    )
)

enriched = (
    metrics.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.*",
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    enriched
    .groupBy("agent_id", "agent_tier")
    .pivot("priority_group", ["group_a", "group_b", "other"])
    .agg(F.min("csat"))
    .orderBy("agent_id", "agent_tier")
)
