from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_level_by_level = Window.partitionBy("team", "level").orderBy("level", "id")
w_team_level_by_level_prior_2 = Window.partitionBy("team", "level").orderBy("level", "id").rowsBetween(-2, -1)
w_team_level_by_level_full = Window.partitionBy("team", "level").orderBy("level", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_adjusters
    .filter(F.length(F.trim(F.col("adjuster_name"))) >= 8)
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

filtered = (
    valid.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.id")) & (F.col("c.status").isin("REJECTED", "OPEN", "PAID")) & (F.col("c.claim_date") >= F.lit("2024-04-15").cast("date")), "left_anti")
)

with_windows = (
    filtered
    .select(
        "*",
        F.round(F.percent_rank().over(w_team_level_by_level), 3).alias("level_percent_rank"),
        F.round(F.avg("level").over(w_team_level_by_level_prior_2), 2).alias("level_avg_2_prior"),
        F.last("level").over(w_team_level_by_level_full).alias("last_level"),
        F.first("level").over(w_team_level_by_level_full).alias("first_level"),
    )
)

team_stats = (
    with_windows
    .groupBy("team")
    .agg(F.avg("last_level").alias("avg_last_level"))
)

vs_group = (
    with_windows.alias("s")
    .join(team_stats.alias("g"), F.col("g.team") == F.col("s.team"), "inner")
    .filter(F.col("s.last_level") > F.col("g.avg_last_level"))
    .select(
        "s.*",
        "g.avg_last_level",
        F.round(F.col("s.last_level") - F.col("g.avg_last_level"), 2).alias("last_level_minus_avg"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "last_level_minus_avg",
        "level_avg_2_prior",
        "level_percent_rank",
    )
    .unpivot(
        ids=["id"],
        values=["last_level_minus_avg", "level_avg_2_prior", "level_percent_rank"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
