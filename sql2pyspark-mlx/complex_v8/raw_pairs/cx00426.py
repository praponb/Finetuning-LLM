from pyspark.sql import functions as F

base = (
    df_adjusters
    .filter((F.col("team").isin("Health", "Travel", "Property")) & (F.col("level") > 2))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.when(F.col("team").isin("Motor", "Travel"), "group_a").when(F.col("team") == "Property", "group_b").otherwise("other").alias("team_group"),
        F.substring(F.trim(F.col("team")), 1, 3).alias("team_prefix"),
    )
)

scored = base.withColumn("team_or_unknown", F.coalesce(F.col("team"), F.lit("UNKNOWN")))

result = (
    scored
    .rollup("team_group", "level", "team_or_unknown")
    .agg(
        F.array_sort(F.collect_set("adjuster_name")).alias("adjuster_name_list"),
        F.max("level").alias("max_level"),
        F.count("*").alias("cnt"),
    )
)
