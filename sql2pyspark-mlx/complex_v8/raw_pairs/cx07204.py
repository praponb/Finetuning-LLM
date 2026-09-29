from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_by_max_days_since_c = Window.partitionBy("agent_id").orderBy("max_days_since_closed_at")
w_agent_id_agent_hired_on_year = Window.partitionBy("agent_id", F.year("agent_hired_on"))

valid = (
    df_tickets
    .filter(F.to_date(F.col("closed_at")) < F.lit("2025-02-14").cast("date"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "tags",
        F.concat(F.col("channel"), F.lit("#"), F.col("id").cast("string")).alias("channel_ref"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
    )
)

with_details = (
    valid.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.agent_name").isNotNull()), "inner")
    .filter(F.col("su.hired_on") >= F.lit("2024-05-02").cast("date"))
    .select("s.*", F.col("su.hired_on").alias("agent_hired_on"))
)

agent_id_stats = (
    with_details
    .groupBy("agent_id")
    .agg(F.max("days_since_closed_at").alias("max_days_since_closed_at"))
)

vs_group = (
    with_details.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.days_since_closed_at") == F.col("g.max_days_since_closed_at"))
    .select(
        "s.*",
        "g.max_days_since_closed_at",
        F.round(F.col("s.days_since_closed_at") - F.col("g.max_days_since_closed_at"), 2).alias("days_since_closed_at_minus_max"),
    )
)

ranked = (
    vs_group
    .withColumn("max_days_since_closed_at_rank", F.rank().over(w_agent_id_by_max_days_since_c))
    .withColumn("rows_in_agent_id_agent_hired_on_year", F.count(F.lit(1)).over(w_agent_id_agent_hired_on_year))
)

per_group = (
    ranked
    .groupBy(
        F.date_format("opened_at", "yyyy-MM").alias("opened_at_ym"),
        "max_days_since_closed_at_rank",
    )
    .agg(
        F.round(F.sum("rows_in_agent_id_agent_hired_on_year") / F.nullif(F.sum("days_since_closed_at"), F.lit(0)), 3).alias("rows_in_agent_id_agent_hired_on_year_to_days_sin"),
        F.max("closed_at").alias("last_closed_at"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 2)
)

scored = (
    per_group
    .filter(F.col("max_days_since_closed_at_rank") < 6)
    .select(
        "opened_at_ym",
        "max_days_since_closed_at_rank",
        "rows_in_agent_id_agent_hired_on_year_to_days_sin",
        "last_closed_at",
        "cnt",
        F.concat(F.col("opened_at_ym"), F.lit("#"), F.col("cnt").cast("string")).alias("opened_at_ym_ref"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("last_closed_at")).alias("days_since_last_closed_at"),
        F.date_trunc("quarter", F.col("last_closed_at")).alias("last_closed_at_quarter"),
    )
)

result = (
    scored
    .select(
        "opened_at_ym",
        "last_closed_at",
        "cnt",
        "days_since_last_closed_at",
    )
    .orderBy(F.col("opened_at_ym").desc(), F.col("cnt").asc_nulls_last())
)
