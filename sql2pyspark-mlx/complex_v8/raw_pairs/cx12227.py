from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tier_team_by_hired_on_ties = Window.partitionBy("tier", "team").orderBy(F.col("hired_on").desc())
w_tier_team_by_hired_on = Window.partitionBy("tier", "team").orderBy(F.col("hired_on").desc(), "id")
w_all_by_hired_on = Window.orderBy("hired_on", "id")
w_all_by_hired_on_ties = Window.orderBy("hired_on")

valid = (
    df_support_agents
    .filter(F.col("team") == "Tech")
    .select(
        "id",
        "team",
        "tier",
        "hired_on",
        F.upper(F.trim(F.col("agent_name"))).alias("agent_name_clean"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.rank().over(w_tier_team_by_hired_on_ties).alias("hired_on_rank"),
        F.datediff(F.col("hired_on"), F.lag("hired_on").over(w_tier_team_by_hired_on)).alias("days_since_prev_hired_on"),
        F.lead("hired_on", 2).over(w_all_by_hired_on).alias("next2_hired_on"),
        F.round(F.cume_dist().over(w_all_by_hired_on_ties), 3).alias("hired_on_cume_dist"),
    )
)

tagged = (
    with_windows
    .select(
        "*",
        F.lower(F.trim(F.col("tier"))).alias("tier_clean"),
        F.length(F.trim(F.col("agent_name_clean"))).alias("agent_name_clean_len"),
    )
)

labeled = (
    tagged
    .filter(F.col("agent_name_clean").isNotNull())
    .select(
        "id",
        "team",
        "tier",
        "hired_on",
        "agent_name_clean",
        "days_since_prev_hired_on",
        "next2_hired_on",
        "hired_on_cume_dist",
        "tier_clean",
        F.round(F.col("hired_on_cume_dist") * 1.5, 2).alias("hired_on_cume_dist_adj"),
        F.quarter(F.col("hired_on")).alias("hired_on_quarter"),
    )
)

result = (
    labeled
    .groupingSets(
        [["team", "hired_on_quarter"], ["team"], ["hired_on_quarter"]],
        "team", "hired_on_quarter",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.min("hired_on_cume_dist_adj").alias("min_hired_on_cume_dist_adj"),
        F.count("*").alias("cnt"),
        F.round(F.sum("hired_on_quarter") / F.nullif(F.sum("hired_on_cume_dist_adj"), F.lit(0)), 3).alias("hired_on_quarter_to_hired_on_cume_dist_adj_ratio"),
    )
    .orderBy(
        F.col("team").asc_nulls_last(),
        F.col("hired_on_quarter").asc_nulls_last(),
    )
)
