from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tier = Window.partitionBy("tier")
w_hired_on_month_team = Window.partitionBy(F.date_trunc("month", "hired_on"), "team")
w_hired_on_quarter_by_hired_on = Window.partitionBy("hired_on_quarter").orderBy(F.col("hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_hired_on_year = Window.partitionBy(F.year("hired_on"))
w_tier_hired_on_quarter_by_hir = Window.partitionBy("tier", "hired_on_quarter").orderBy(F.col("hired_on").desc(), "id")
w_all_by_hired_on = Window.orderBy("hired_on", "id")

valid_agents = (
    df_support_agents
    .filter((F.lower(F.col("agent_name")).like("%a %")) & (F.col("tier") == "T2"))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.quarter(F.col("hired_on")).alias("hired_on_quarter"),
    )
)

with_trends = (
    valid_agents
    .select(
        "*",
        F.round(F.col("hired_on_quarter") / F.nullif(F.sum("hired_on_quarter").over(w_tier), F.lit(0)), 4).alias("hired_on_quarter_share_of_tier"),
        F.max("hired_on_quarter").over(w_hired_on_month_team).alias("hired_on_month_team_max_hired_on_quarter"),
        F.sum("hired_on_quarter").over(w_hired_on_month_team).alias("hired_on_month_team_sum_hired_on_quarter"),
        F.last("hired_on_quarter").over(w_hired_on_quarter_by_hired_on).alias("last_hired_on_quarter"),
        F.round(F.col("hired_on_quarter") - F.avg("hired_on_quarter").over(w_hired_on_year), 2).alias("hired_on_quarter_vs_hired_on_year_avg"),
        F.ntile(5).over(w_tier_hired_on_quarter_by_hir).alias("hired_on_quintile"),
        F.round(F.col("hired_on_quarter") / F.nullif(F.sum("hired_on_quarter").over(w_hired_on_year), F.lit(0)), 4).alias("hired_on_quarter_share_of_hired_on_year"),
        F.row_number().over(w_all_by_hired_on).alias("hired_on_seq"),
        F.min("hired_on_quarter").over(w_hired_on_month_team).alias("hired_on_month_team_min_hired_on_quarter"),
        F.sum("hired_on_quarter").over(w_hired_on_year).alias("hired_on_year_sum_hired_on_quarter"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        "hired_on_quarter",
        "hired_on_month_team_max_hired_on_quarter",
        "hired_on_month_team_sum_hired_on_quarter",
        "last_hired_on_quarter",
        "hired_on_quarter_vs_hired_on_year_avg",
        "hired_on_quintile",
        "hired_on_seq",
        "hired_on_month_team_min_hired_on_quarter",
        "hired_on_year_sum_hired_on_quarter",
        F.coalesce(F.col("tier"), F.lit("UNKNOWN")).alias("tier_or_unknown"),
    )
    .orderBy("hired_on_quintile", "id")
    .limit(20)
)
