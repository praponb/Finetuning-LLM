from pyspark.sql import functions as F

valid_agents = (
    df_support_agents
    .filter(F.col("team") == "Tech")
    .select(
        "id",
        "team",
        "tier",
        "hired_on",
        F.length(F.trim(F.col("team"))).alias("team_len"),
        F.length(F.trim(F.col("agent_name"))).alias("agent_name_len"),
        F.lower(F.regexp_replace(F.trim(F.col("team")), " ", "_")).alias("team_slug"),
    )
)

screened = (
    valid_agents
    .crossJoin(valid_agents.agg(F.max("agent_name_len").alias("overall_max_agent_name_len")))
    .filter(F.col("agent_name_len") < F.col("overall_max_agent_name_len"))
    .drop("overall_max_agent_name_len")
)

result = (
    screened
    .rollup("agent_name_len", "team")
    .agg(
        F.round(F.sum("agent_name_len") / F.nullif(F.sum("team_len"), F.lit(0)), 3).alias("agent_name_len_to_team_len_ratio"),
    )
)
