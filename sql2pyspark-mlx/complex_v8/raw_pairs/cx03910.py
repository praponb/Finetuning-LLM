from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("priority", "agent_id").orderBy(F.col("requester_id").desc(), F.col("id").desc())

scoped = (
    df_tickets
    .filter(F.col("opened_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "tags",
        F.concat_ws(": ", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
)

unique_rows = (
    scoped
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

priority_stats = (
    unique_rows
    .groupBy("priority")
    .agg(F.avg("days_since_opened_at").alias("avg_days_since_opened_at"))
)

above_average = (
    unique_rows.alias("s")
    .join(priority_stats.alias("g"), F.col("g.priority") == F.col("s.priority"), "inner")
    .filter(F.col("s.days_since_opened_at") > F.col("g.avg_days_since_opened_at"))
    .select(
        "s.*",
        "g.avg_days_since_opened_at",
        F.round(F.col("s.days_since_opened_at") - F.col("g.avg_days_since_opened_at"), 2).alias("days_since_opened_at_minus_avg"),
    )
)

result = (
    above_average
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "tags",
        "days_since_opened_at",
        "avg_days_since_opened_at",
        "days_since_opened_at_minus_avg",
    )
)
