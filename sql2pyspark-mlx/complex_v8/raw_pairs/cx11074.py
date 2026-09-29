from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("opened_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_tickets
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
        (F.col("csat") + F.col("requester_id")).alias("csat_plus_requester_id"),
    )
)

with_prev = valid.withColumn("prev_opened_at", F.lag("opened_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_priority_running))

per_group = (
    sessionized
    .groupBy(F.to_date("opened_at").alias("opened_at_day"))
    .agg(
        F.sum(F.when(F.col("priority") != "P4", F.col("starts_session")).otherwise(0)).alias("starts_session_priority_p4"),
        F.countDistinct("priority").alias("distinct_prioritys"),
        F.max("session_no").alias("max_session_no"),
        F.countDistinct("channel").alias("distinct_channels"),
    )
)

enriched = (
    per_group
    .filter(F.col("distinct_prioritys") < 8)
    .select(
        "opened_at_day",
        "starts_session_priority_p4",
        "distinct_prioritys",
        "max_session_no",
        F.when(F.col("distinct_prioritys") >= 9, "large").when(F.col("distinct_prioritys") >= 5, "medium").otherwise("small").alias("distinct_prioritys_tier"),
    )
)

result = (
    enriched
    .rollup("distinct_prioritys", "starts_session_priority_p4")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.min("opened_at_day").alias("first_opened_at_day"),
        F.datediff(F.max("opened_at_day"), F.min("opened_at_day")).alias("opened_at_day_span_days"),
        F.percentile_approx("distinct_prioritys", 0.5).alias("median_distinct_prioritys"),
    )
)
