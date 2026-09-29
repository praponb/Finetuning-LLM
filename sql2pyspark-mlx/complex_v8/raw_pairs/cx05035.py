from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("closed_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_team_agent_id_by_sessi = Window.partitionBy("agent_team", "agent_id").orderBy("session_start", "agent_id", "session_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_team_agent_id_by_sessi_2 = Window.partitionBy("agent_team", "agent_id").orderBy("session_start", "agent_id", "session_no").rowsBetween(-6, Window.currentRow)
w_agent_team_agent_id_by_sessi_3 = Window.partitionBy("agent_team", "agent_id").orderBy("session_start", "agent_id", "session_no").rowsBetween(-1, Window.currentRow)
w_session_end_to_date = Window.partitionBy(F.to_date("session_end"))

cleaned = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "tags",
    )
)

with_prev = cleaned.withColumn("prev_closed_at", F.lag("closed_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_closed_at").isNull() | (F.col("closed_at") > F.col("prev_closed_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_agent_id_running))

session_summary = (
    sessionized
    .groupBy("agent_id", "session_no")
    .agg(
        F.min("closed_at").alias("session_start"),
        F.max("closed_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("closed_at")) - F.unix_timestamp(F.min("closed_at"))) / 60, 1).alias("duration_min"),
    )
)

with_parent_agent = (
    session_summary.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.agent_id",
        "s.session_no",
        "s.session_start",
        "s.session_end",
        "s.duration_min",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

metrics = (
    with_parent_agent
    .select(
        "*",
        F.first("duration_min", ignorenulls=True).over(w_agent_team_agent_id_by_sessi).alias("first_known_duration_min"),
        F.max("session_no").over(w_agent_team_agent_id_by_sessi_2).alias("session_no_max_7"),
        F.round(F.avg("session_no").over(w_agent_team_agent_id_by_sessi_3), 2).alias("session_no_avg_2"),
        F.count(F.lit(1)).over(w_session_end_to_date).alias("rows_in_session_end_to_date"),
        F.size(F.collect_set("agent_team").over(w_session_end_to_date)).alias("distinct_agent_team_in_session_end_to_date"),
    )
)

result = (
    metrics
    .select(
        "agent_id",
        "session_start",
        "session_end",
        "duration_min",
        "agent_team",
        "agent_hired_on",
        "agent_name",
        "session_no_max_7",
        "session_no_avg_2",
        "rows_in_session_end_to_date",
        "distinct_agent_team_in_session_end_to_date",
        F.least(F.col("duration_min"), F.col("session_no_avg_2")).alias("least_duration_min_session_no_avg_2"),
        F.upper(F.trim(F.col("agent_name"))).alias("agent_name_clean"),
    )
    .orderBy(F.col("agent_name_clean").desc_nulls_first())
)
