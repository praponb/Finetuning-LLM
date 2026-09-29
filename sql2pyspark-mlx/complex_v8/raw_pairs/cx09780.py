from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_tier_by_hired_on_ties = Window.partitionBy("team", "tier").orderBy(F.col("hired_on").desc())
w_team_by_hired_on_running = Window.partitionBy("team").orderBy("hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_tier_by_hired_on = Window.partitionBy("tier").orderBy("hired_on", "id")
w_team_by_hired_on = Window.partitionBy("team").orderBy("hired_on", "id")
w_team_by_hired_on_full = Window.partitionBy("team").orderBy("hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_team_tier_by_hired_on = Window.partitionBy("team", "tier").orderBy(F.col("hired_on").desc(), "id")
w_tier_by_hired_on_ties = Window.partitionBy("tier").orderBy("hired_on")

valid = (
    df_support_agents
    .filter(F.col("team") != "Onboarding")
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.lower(F.regexp_replace(F.trim(F.col("agent_name")), " ", "_")).alias("agent_name_slug"),
    )
)

windowed = (
    valid
    .select(
        "*",
        F.dense_rank().over(w_team_tier_by_hired_on_ties).alias("hired_on_dense_rank"),
        F.round(F.cume_dist().over(w_team_tier_by_hired_on_ties), 3).alias("hired_on_cume_dist"),
        F.last("hired_on", ignorenulls=True).over(w_team_by_hired_on_running).alias("last_known_hired_on"),
        F.lead("hired_on").over(w_tier_by_hired_on).alias("next_hired_on"),
        F.round(F.cume_dist().over(w_tier_by_hired_on), 3).alias("hired_on_cume_dist_2"),
        F.dense_rank().over(w_team_by_hired_on).alias("hired_on_dense_rank_2"),
        F.nth_value("hired_on", 2).over(w_team_by_hired_on_full).alias("second_hired_on"),
        F.ntile(3).over(w_team_tier_by_hired_on).alias("hired_on_tercile"),
        F.rank().over(w_tier_by_hired_on_ties).alias("hired_on_rank"),
        F.lead("hired_on", 3).over(w_team_tier_by_hired_on).alias("next3_hired_on"),
        F.row_number().over(w_tier_by_hired_on).alias("rn"),
        F.last("hired_on").over(w_team_by_hired_on_full).alias("last_hired_on"),
        F.dense_rank().over(w_tier_by_hired_on_ties).alias("hired_on_dense_rank_3"),
    )
)

labeled = (
    windowed
    .filter(F.col("hired_on_dense_rank") > 4)
    .withColumn("agent_name_slug_len", F.length(F.trim(F.col("agent_name_slug"))))
)

grouped = (
    labeled
    .filter(F.col("tier").isNotNull())
    .groupBy(
        F.date_trunc("month", "last_known_hired_on").alias("last_known_hired_on_month"),
    )
    .agg(
        F.sum("agent_name_slug_len").alias("total_agent_name_slug_len"),
        F.round(100 * F.count(F.when(F.col("team") == "VIP", 1)) / F.count("*"), 1).alias("pct_team_vip"),
        F.avg("hired_on_cume_dist_2").alias("avg_hired_on_cume_dist_2_check"),
    )
    .filter(F.col("avg_hired_on_cume_dist_2_check") > 0.4)
    .drop("avg_hired_on_cume_dist_2_check")
)

result = (
    grouped
    .select("pct_team_vip", "total_agent_name_slug_len")
    .distinct()
    .orderBy(F.col("total_agent_name_slug_len").desc(), "pct_team_vip")
)
