from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("team").orderBy(F.col("level").desc(), F.col("id").desc())
w_level_team_by_level_running = Window.partitionBy("level", "team").orderBy("level", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_level_team_by_level = Window.partitionBy("level", "team").orderBy("level", "id")
w_level_team_by_level_ties = Window.partitionBy("level", "team").orderBy("level")

recent = (
    df_adjusters
    .filter(F.lower(F.col("adjuster_name")).like("%va%"))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

deduped = (
    recent
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

sequenced = (
    deduped
    .select(
        "id",
        "team",
        "level",
        F.first("level", ignorenulls=True).over(w_level_team_by_level_running).alias("first_known_level"),
        F.lead("level", 3, 0).over(w_level_team_by_level).alias("next3_level"),
        F.row_number().over(w_level_team_by_level).alias("seq_no"),
        F.round(F.percent_rank().over(w_level_team_by_level_ties), 3).alias("level_percent_rank"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "team",
        "level",
        "first_known_level",
        "next3_level",
        "seq_no",
        "level_percent_rank",
        F.length(F.trim(F.col("team"))).alias("team_len"),
        F.abs(F.col("next3_level") - 3).alias("next3_level_distance"),
    )
    .orderBy("team", F.col("first_known_level").asc())
)
