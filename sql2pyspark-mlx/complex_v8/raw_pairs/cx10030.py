from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("team_or_unknown").orderBy(F.col("level").desc(), F.col("id").desc())
w_level = Window.partitionBy("level")

valid_adjusters = (
    df_adjusters
    .filter((F.col("team").isin("Travel", "Health", "Motor")) & (F.col("adjuster_name").isNotNull()))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

scored = (
    valid_adjusters
    .select(
        "id",
        "level",
        F.coalesce(F.col("level"), F.lit(0)).alias("level_filled"),
        F.coalesce(F.col("team"), F.lit("UNKNOWN")).alias("team_or_unknown"),
    )
)

latest = (
    scored
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

sequenced = (
    latest
    .withColumn("level_min_level_filled", F.min("level_filled").over(w_level))
    .withColumn("rows_in_level", F.count(F.lit(1)).over(w_level))
)

result = (
    sequenced
    .groupingSets(
        [["team_or_unknown", "level_filled"], ["team_or_unknown"], ["level_filled"]],
        "team_or_unknown", "level_filled",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.sum("rows_in_level") / F.nullif(F.sum("level"), F.lit(0)), 3).alias("rows_in_level_to_level_ratio"),
        F.round(F.stddev("level_min_level_filled"), 2).alias("stddev_level_min_level_filled"),
    )
)
