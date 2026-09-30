from pyspark.sql import functions as F

covered = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.csat").alias("prior_csat_12h"),
    )
)

combined_details = (
    covered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.prior_rows_12h",
        "s.prior_csat_12h",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
    )
)

labeled = (
    combined_details
    .filter(F.col("agent_hired_on").between(F.lit("2024-05-02").cast("date"), F.lit("2025-03-30").cast("date")))
    .select(
        "id",
        "agent_id",
        "prior_csat_12h",
        "agent_hired_on",
        "agent_team",
        F.least(F.col("prior_csat_12h"), F.col("prior_rows_12h")).alias("least_prior_csat_12h_prior_rows_12h"),
        F.length(F.trim(F.col("agent_team"))).alias("agent_team_len"),
        F.lower(F.trim(F.col("agent_team"))).alias("agent_team_clean"),
    )
)

with_details = (
    labeled.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.prior_csat_12h",
        "s.least_prior_csat_12h_prior_rows_12h",
        "s.agent_team_clean",
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on_2"),
    )
)

result = (
    with_details
    .select(
        "id",
        "agent_id",
        "least_prior_csat_12h_prior_rows_12h",
        "agent_team_clean",
        "agent_tier",
        "agent_hired_on_2",
        F.length(F.trim(F.col("agent_team_clean"))).alias("agent_team_clean_len"),
        F.lower(F.regexp_replace(F.trim(F.col("agent_team_clean")), " ", "_")).alias("agent_team_clean_slug"),
    )
    .orderBy(
        F.col("least_prior_csat_12h_prior_rows_12h").desc(),
        F.col("agent_hired_on_2").desc(),
    )
)
