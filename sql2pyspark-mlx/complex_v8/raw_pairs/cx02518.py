from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_tier = Window.partitionBy("team", "tier")
w_all_by_hired_on = Window.orderBy("hired_on", "id")
w_all_by_hired_on_ties = Window.orderBy("hired_on")
w_all_by_hired_on_running = Window.orderBy("hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_support_agents
    .filter((F.col("tier") == "T1") & (F.col("hired_on").between(F.lit("2024-07-09").cast("date"), F.lit("2025-03-30").cast("date"))))
    .select(
        "id",
        "team",
        "tier",
        "hired_on",
        F.when(F.col("tier").isin("T1", "T3"), "group_a").when(F.col("tier") == "T2", "group_b").otherwise("other").alias("tier_group"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("hired_on")).alias("days_since_hired_on"),
    )
)

with_trends = (
    cleaned
    .select(
        "id",
        "team",
        "tier_group",
        "days_since_hired_on",
        F.max("days_since_hired_on").over(w_team_tier).alias("team_tier_max_days_since_hired_on"),
        F.lead("days_since_hired_on").over(w_all_by_hired_on).alias("next_days_since_hired_on"),
        F.count(F.lit(1)).over(w_team_tier).alias("rows_in_team_tier"),
        F.dense_rank().over(w_all_by_hired_on_ties).alias("hired_on_dense_rank"),
        F.round(F.col("days_since_hired_on") / F.nullif(F.sum("days_since_hired_on").over(w_team_tier), F.lit(0)), 4).alias("days_since_hired_on_share_of_team_tier"),
        F.first("days_since_hired_on", ignorenulls=True).over(w_all_by_hired_on_running).alias("first_known_days_since_hired_on"),
    )
)

result = (
    with_trends
    .rollup("hired_on_dense_rank", "tier_group", "team")
    .agg(
        F.sum(F.when(F.col("team") == "VIP", F.col("team_tier_max_days_since_hired_on"))).alias("team_tier_max_days_since_hired_on_team_vip"),
        F.sum("rows_in_team_tier").alias("total_rows_in_team_tier"),
    )
    .orderBy(
        F.col("hired_on_dense_rank").asc_nulls_last(),
        F.col("tier_group").asc_nulls_last(),
        F.col("team").asc_nulls_last(),
    )
)
