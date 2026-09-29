from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_level = Window.partitionBy("team", "level")
w_all_by_level_moving_3 = Window.orderBy("level", "id").rowsBetween(-3, Window.currentRow)
w_all_by_level = Window.orderBy("level", "id")
w_all_by_level_prior_2 = Window.orderBy("level", "id").rowsBetween(-2, -1)

scoped_adjusters = (
    df_adjusters
    .filter(F.upper(F.trim(F.col("adjuster_name"))).like("E%"))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.concat(F.col("team"), F.lit("#"), F.col("level").cast("string")).alias("team_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("team")), " ", "_")).alias("team_slug"),
        F.lower(F.trim(F.col("adjuster_name"))).alias("adjuster_name_clean"),
    )
)

filtered = (
    scoped_adjusters.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.id")) & (F.col("c.claimed_amount") > 17759.3) & (F.col("c.approved_amount") > 0.0), "left_semi")
    .filter(F.length(F.trim(F.col("adjuster_name_clean"))) >= 8)
)

ranked = (
    filtered
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        "team_slug",
        F.round(F.avg("level").over(w_team_level), 2).alias("team_level_avg_level"),
        F.round(F.avg("level").over(w_all_by_level_moving_3), 2).alias("level_avg_4"),
        F.lead("level", 3).over(w_all_by_level).alias("next3_level"),
        F.min("level").over(w_team_level).alias("team_level_min_level"),
        F.round(F.avg("level").over(w_all_by_level_prior_2), 2).alias("level_avg_2_prior"),
    )
)

scored = (
    ranked
    .filter(F.col("team_level_avg_level") < 3.4)
    .withColumn("team_or_unknown", F.coalesce(F.col("team"), F.lit("UNKNOWN")))
)

result = (
    scored
    .select("level")
    .distinct()
    .orderBy("level")
)
