from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_or_unknown_team_by_leve = Window.partitionBy("team_or_unknown", "team").orderBy("level_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_team_or_unknown_by_level_fil = Window.partitionBy("team_or_unknown").orderBy(F.col("level_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_level = Window.partitionBy("level")
w_team_or_unknown_by_level_fil_2 = Window.partitionBy("team_or_unknown").orderBy(F.col("level_filled").desc())

scoped = (
    df_adjusters
    .filter((F.length(F.trim(F.col("adjuster_name"))) >= 7) & (F.col("level").between(1, 3)))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

tagged = (
    scoped
    .filter(F.col("level") <= 3)
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.coalesce(F.col("team"), F.lit("UNKNOWN")).alias("team_or_unknown"),
        F.coalesce(F.col("level"), F.lit(0)).alias("level_filled"),
    )
)

ranked = (
    tagged
    .select(
        "*",
        F.sum("level").over(w_team_or_unknown_team_by_leve).alias("running_level"),
        F.nth_value("level", 2).over(w_team_or_unknown_by_level_fil).alias("second_level"),
        F.round(F.col("level") / F.nullif(F.sum("level").over(w_level), F.lit(0)), 4).alias("level_share_of_level"),
        F.round(F.percent_rank().over(w_team_or_unknown_by_level_fil_2), 3).alias("level_filled_percent_rank"),
    )
)

labeled = (
    ranked
    .select(
        "id",
        "adjuster_name",
        "team_or_unknown",
        "level_filled",
        "second_level",
        "level_share_of_level",
        "level_filled_percent_rank",
        F.when(F.col("team_or_unknown").isin("Property", "Health"), "group_a").when(F.col("team_or_unknown") == "UNKNOWN", "group_b").otherwise("other").alias("team_or_unknown_group"),
        F.concat(F.col("adjuster_name"), F.lit("#"), F.col("running_level").cast("string")).alias("adjuster_name_ref"),
    )
)

result = (
    labeled
    .select(
        "id",
        F.col("second_level").alias("second_level_v"),
        F.col("level_filled").alias("level_filled_v"),
    )
    .unpivot(
        ids=["id"],
        values=["second_level_v", "level_filled_v"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "field")
)
