from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("team", "team_group").orderBy(F.col("level_distance").desc(), F.col("id").desc())

prepared_adjusters = (
    df_adjusters
    .filter((F.upper(F.trim(F.col("adjuster_name"))).like("G%")) | (F.col("level") >= 1))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.when(F.col("team").isin("Travel", "Motor"), "group_a").when(F.col("team") == "Property", "group_b").otherwise("other").alias("team_group"),
        F.abs(F.col("level") - 3).alias("level_distance"),
    )
)

deduped = (
    prepared_adjusters
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    deduped
    .filter(F.col("team") == "Health")
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        "team_group",
        "level_distance",
        F.when(F.col("team").isin("Health", "Property"), "group_a").when(F.col("team") == "Travel", "group_b").otherwise("other").alias("team_group_2"),
    )
    .orderBy(F.col("team_group_2").desc())
)
