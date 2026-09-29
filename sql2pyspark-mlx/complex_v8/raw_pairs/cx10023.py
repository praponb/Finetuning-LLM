from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id")
w_all_by_days_since_closed_at = Window.orderBy("days_since_closed_at", "id")
w_csat_distance_channel_by_day = Window.partitionBy("csat_distance", "channel").orderBy(F.col("days_since_closed_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_tickets
    .filter(F.col("priority") == "P1")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
    )
)

tagged = (
    valid
    .select(
        "*",
        F.upper(F.trim(F.col("priority"))).alias("priority_clean"),
        F.concat_ws("-", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
        F.date_format(F.col("closed_at"), "MMM yyyy").alias("closed_at_label"),
    )
)

derived = tagged.withColumn("csat_distance", F.abs(F.col("csat") - 3))

agent_id_stats = (
    derived
    .groupBy("agent_id")
    .agg(F.avg("days_since_closed_at").alias("avg_days_since_closed_at"))
)

above_average = (
    derived.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.days_since_closed_at") > F.col("g.avg_days_since_closed_at"))
    .select(
        "s.*",
        "g.avg_days_since_closed_at",
        F.round(F.col("s.days_since_closed_at") - F.col("g.avg_days_since_closed_at"), 2).alias("days_since_closed_at_minus_avg"),
    )
)

windowed = (
    above_average
    .select(
        "*",
        F.count(F.lit(1)).over(w_agent_id).alias("rows_in_agent_id"),
        F.row_number().over(w_all_by_days_since_closed_at).alias("days_since_closed_at_seq"),
        F.nth_value("closed_at", 2).over(w_csat_distance_channel_by_day).alias("second_closed_at"),
    )
)

result = (
    windowed
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "tags",
        "days_since_closed_at",
        "priority_clean",
        "priority_channel_label",
        "closed_at_label",
        "csat_distance",
        "avg_days_since_closed_at",
        "days_since_closed_at_minus_avg",
        "rows_in_agent_id",
        "days_since_closed_at_seq",
        "second_closed_at",
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
    )
    .orderBy("days_since_closed_at_minus_avg")
)
