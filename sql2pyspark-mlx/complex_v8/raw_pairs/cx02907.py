from pyspark.sql import functions as F

prepared = (
    df_adjusters
    .filter(F.upper(F.trim(F.col("adjuster_name"))).like("G%"))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

labeled = prepared.select("*", F.abs(F.col("level") - 3).alias("level_distance"))

per_group = (
    labeled
    .groupBy("team", "level")
    .agg(
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("team")).alias("team_list"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

qualified = (
    per_group
    .crossJoin(per_group.agg(F.avg("level").alias("overall_avg_level")))
    .filter(F.col("level") > F.col("overall_avg_level"))
    .drop("overall_avg_level")
)

result = (
    qualified
    .select(
        "team",
        "level",
        "distinct_ids",
        "row_count",
    )
    .unpivot(
        ids=["team", "level"],
        values=["distinct_ids", "row_count"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("team", "level", "field")
)
