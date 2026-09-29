from pyspark.sql import functions as F

scoped = (
    df_adjusters
    .filter((F.col("level") < 3) & (F.col("team") != "Health"))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.concat(F.col("adjuster_name"), F.lit("#"), F.col("id").cast("string")).alias("adjuster_name_ref"),
        F.concat_ws("-", F.col("adjuster_name"), F.col("team")).alias("adjuster_name_team_label"),
    )
)

annotated = (
    scoped
    .filter(F.col("team").isNotNull())
    .withColumn("adjuster_name_adjuster_name_team_label_label", F.concat_ws(" / ", F.col("adjuster_name"), F.col("adjuster_name_team_label")))
)

result = (
    annotated
    .select(
        "id",
        "adjuster_name",
        "adjuster_name_ref",
        "adjuster_name_team_label",
        "adjuster_name_adjuster_name_team_label_label",
    )
    .orderBy(
        F.col("id").desc_nulls_first(),
        F.col("adjuster_name_team_label").desc(),
    )
)
