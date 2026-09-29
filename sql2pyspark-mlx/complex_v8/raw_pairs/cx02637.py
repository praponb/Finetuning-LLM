from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_hired_on_month_agent_t = Window.partitionBy(F.date_trunc("month", "agent_hired_on"), "agent_tier")
w_agent_tier_by_csat_distance = Window.partitionBy("agent_tier").orderBy("csat_distance")
w_agent_id_by_opened_at_runnin = Window.partitionBy("agent_id").orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_by_opened_at_prior = Window.partitionBy("agent_id").orderBy(F.col("opened_at").desc(), "id").rowsBetween(-6, -1)
w_agent_id_by_opened_at_ties = Window.partitionBy("agent_id").orderBy(F.col("opened_at").desc())
w_agent_tier_by_csat_distance_2 = Window.partitionBy("agent_tier").orderBy("csat_distance", "id").rowsBetween(-6, Window.currentRow)
w_agent_id_by_opened_at_moving = Window.partitionBy("agent_id").orderBy(F.col("opened_at").desc(), "id").rowsBetween(-1, Window.currentRow)

base = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "csat",
        "tags",
        F.abs(F.col("csat") - 3).alias("csat_distance"),
    )
)

enriched = (
    base.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.upper(F.trim(F.col("su.agent_name"))).like("K%")), "left")
    .select(
        "s.*",
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

ranked = (
    enriched
    .select(
        "*",
        F.round(F.col("csat_distance") - F.avg("csat_distance").over(w_agent_hired_on_month_agent_t), 2).alias("csat_distance_vs_agent_hired_on_month_agent_tier"),
        F.rank().over(w_agent_tier_by_csat_distance).alias("csat_distance_rank"),
        F.first("opened_at", ignorenulls=True).over(w_agent_id_by_opened_at_runnin).alias("first_known_opened_at"),
        F.count(F.lit(1)).over(w_agent_hired_on_month_agent_t).alias("rows_in_agent_hired_on_month_agent_tier"),
        F.round(F.avg("csat").over(w_agent_id_by_opened_at_prior), 2).alias("csat_avg_6_prior"),
        F.size(F.collect_set("agent_id").over(w_agent_hired_on_month_agent_t)).alias("distinct_agent_id_in_agent_hired_on_month_agent_"),
        F.rank().over(w_agent_id_by_opened_at_ties).alias("opened_at_rank"),
        F.round(F.avg("csat").over(w_agent_tier_by_csat_distance_2), 2).alias("csat_avg_7"),
        F.round(F.avg("csat").over(w_agent_id_by_opened_at_moving), 2).alias("csat_avg_2"),
        F.round(F.col("csat_distance") / F.nullif(F.sum("csat_distance").over(w_agent_hired_on_month_agent_t), F.lit(0)), 4).alias("csat_distance_share_of_agent_hired_on_month_agen"),
    )
)

first_rows = (
    ranked
    .filter(F.col("opened_at_rank") <= 3)
    .drop("opened_at_rank")
)

result = (
    first_rows
    .select(
        "id",
        "agent_id",
        "opened_at",
        "csat",
        "tags",
        "agent_name",
        "agent_hired_on",
        "csat_distance_vs_agent_hired_on_month_agent_tier",
        "csat_distance_rank",
        "first_known_opened_at",
        "csat_avg_6_prior",
        "csat_avg_7",
        "csat_avg_2",
        "csat_distance_share_of_agent_hired_on_month_agen",
        F.when(F.col("agent_tier").isin("T1", "T2"), "group_a").when(F.col("agent_tier") == "T3", "group_b").otherwise("other").alias("agent_tier_group"),
        F.abs(F.col("csat_avg_2") - 2.6).alias("csat_avg_2_distance"),
    )
)
