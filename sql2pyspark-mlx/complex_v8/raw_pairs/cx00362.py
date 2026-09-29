from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_closed_at_year_by_opened_at = Window.partitionBy(F.year("closed_at")).orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id = Window.partitionBy("agent_id")
w_closed_at_year_by_opened_at_2 = Window.partitionBy(F.year("closed_at")).orderBy("opened_at")
w_dedup = Window.partitionBy("agent_id", "priority").orderBy(F.col("running_csat").desc(), F.col("id").desc())

base = (
    df_tickets
    .filter(F.col("requester_id") < 19)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
    )
)

sequenced = (
    base
    .select(
        "id",
        "agent_id",
        "priority",
        "tags",
        F.sum("csat").over(w_closed_at_year_by_opened_at).alias("running_csat"),
        F.round(F.col("csat") - F.avg("csat").over(w_agent_id), 2).alias("csat_vs_agent_id_avg"),
        F.round(F.percent_rank().over(w_closed_at_year_by_opened_at_2), 3).alias("opened_at_percent_rank"),
    )
)

unique_rows = (
    sequenced
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

joined = (
    unique_rows.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.team") != "Onboarding"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.priority",
        "s.tags",
        "s.running_csat",
        "s.opened_at_percent_rank",
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    joined
    .cube("priority", "agent_tier")
    .agg(
        F.round(F.stddev("running_csat"), 2).alias("stddev_running_csat"),
        F.round(F.avg("opened_at_percent_rank"), 2).alias("avg_opened_at_percent_rank"),
        F.count("*").alias("cnt"),
    )
    .select(
        F.coalesce(F.col("priority"), F.lit("ALL")).alias("priority"),
        F.coalesce(F.col("agent_tier"), F.lit("ALL")).alias("agent_tier"),
        "stddev_running_csat",
        "avg_opened_at_percent_rank",
        "cnt",
    )
    .orderBy(
        F.col("priority").asc_nulls_last(),
        F.col("agent_tier").asc_nulls_last(),
    )
)
