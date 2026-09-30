from pyspark.sql import functions as F

scoped_agents = (
    df_support_agents
    .filter(F.col("tier").isin("T3", "T1"))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("hired_on")).alias("days_since_hired_on"),
    )
)

tier_stats = (
    scoped_agents
    .groupBy("tier")
    .agg(F.max("days_since_hired_on").alias("max_days_since_hired_on"))
)

above_average = (
    scoped_agents.alias("s")
    .join(tier_stats.alias("g"), F.col("g.tier") == F.col("s.tier"), "inner")
    .filter(F.col("s.days_since_hired_on") == F.col("g.max_days_since_hired_on"))
    .select(
        "s.*",
        "g.max_days_since_hired_on",
        F.round(F.col("s.days_since_hired_on") - F.col("g.max_days_since_hired_on"), 2).alias("days_since_hired_on_minus_max"),
    )
)

result = (
    above_average
    .groupingSets(
        [["team", "tier"], ["team"], []],
        "team", "tier",
    )
    .agg(
        F.sum(F.when(F.col("tier") != "T1", 1).otherwise(0)).alias("n_tier_t1"),
        F.sum("days_since_hired_on_minus_max").alias("total_days_since_hired_on_minus_max"),
    )
)
