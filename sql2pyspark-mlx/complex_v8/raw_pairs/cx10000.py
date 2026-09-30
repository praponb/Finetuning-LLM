from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("team", "team_or_unknown").orderBy(F.col("days_since_hired_on").desc(), F.col("id").desc())

base_agents = (
    df_support_agents
    .filter((F.col("team") == "Tech") & (F.col("hired_on") < F.lit("2025-02-27").cast("date")))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.coalesce(F.col("team"), F.lit("UNKNOWN")).alias("team_or_unknown"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("hired_on")).alias("days_since_hired_on"),
    )
)

filtered = (
    base_agents.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.id")) & (~F.array_contains(F.col("t.tags"), "upgrade")) & (F.hour(F.col("t.closed_at")).between(8, 13)), "left_semi")
    .filter(F.col("team") != "Onboarding")
)

latest_per_key = (
    filtered
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

stats = (
    latest_per_key
    .filter(F.col("team_or_unknown").isin("Tech", "Billing"))
    .groupBy("team", "team_or_unknown")
    .agg(
        F.sum(F.when(F.col("team") != "Billing", F.col("days_since_hired_on"))).alias("days_since_hired_on_team_billing"),
        F.max("days_since_hired_on").alias("max_days_since_hired_on"),
    )
)

result = (
    stats
    .select(
        "team",
        "team_or_unknown",
        F.col("max_days_since_hired_on").alias("m_max_days_since_hired_on"),
        F.col("days_since_hired_on_team_billing").alias("days_since_hired_on_team_billing_v"),
    )
    .unpivot(
        ids=["team", "team_or_unknown"],
        values=["m_max_days_since_hired_on", "days_since_hired_on_team_billing_v"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
)
