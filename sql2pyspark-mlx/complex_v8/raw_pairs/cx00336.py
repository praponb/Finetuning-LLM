from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_session_start_prior_3 = Window.orderBy("session_start", "agent_id", "burst_id").rowsBetween(-3, -1)
w_agent_id_2 = Window.partitionBy("agent_id")

cleaned_tickets = (
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
        F.quarter(F.col("opened_at")).alias("opened_at_quarter"),
    )
)

with_prev = cleaned_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_agent_id_running))

session_summary = (
    sessionized
    .groupBy("agent_id", "burst_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

ranked = (
    session_summary
    .select(
        "agent_id",
        "burst_id",
        "session_start",
        "session_end",
        "n_events",
        F.max("duration_min").over(w_all_by_session_start_prior_3).alias("duration_min_max_3_prior"),
        F.min("duration_min").over(w_agent_id_2).alias("agent_id_min_duration_min"),
    )
)

filtered = (
    ranked
    .crossJoin(ranked.agg(F.max("burst_id").alias("overall_max_burst_id")))
    .filter(F.col("burst_id") < F.col("overall_max_burst_id"))
    .drop("overall_max_burst_id")
)

result = (
    filtered
    .select(
        "agent_id",
        "burst_id",
        "duration_min_max_3_prior",
        "agent_id_min_duration_min",
    )
    .unpivot(
        ids=["agent_id", "burst_id"],
        values=["duration_min_max_3_prior", "agent_id_min_duration_min"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
