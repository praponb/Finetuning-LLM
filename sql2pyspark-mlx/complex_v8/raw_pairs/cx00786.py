from pyspark.sql import functions as F

prepared = (
    df_adjusters
    .filter((F.col("level") >= 2) & (F.col("team").isNotNull()))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.upper(F.trim(F.col("adjuster_name"))).alias("adjuster_name_clean"),
        F.coalesce(F.col("level"), F.lit(0)).alias("level_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("adjuster_name")), " ", "_")).alias("adjuster_name_slug"),
    )
)

rollup_base = (
    prepared
    .groupBy("level_filled", "team", "level")
    .agg(
        F.sum(F.when(F.col("team") == "Travel", 1).otherwise(0)).alias("n_team_travel"),
        F.sum("level_filled").alias("total_level_filled"),
        F.array_sort(F.collect_set("team")).alias("team_list"),
        F.round(F.stddev("level_filled"), 2).alias("stddev_level_filled"),
    )
)

enriched = rollup_base.select("*", F.substring(F.trim(F.col("team")), 1, 4).alias("team_prefix"))

per_label = (
    enriched
    .select(
        "level_filled",
        "team",
        "level",
        "n_team_travel",
        "total_level_filled",
        "stddev_level_filled",
        "team_prefix",
        F.explode("team_list").alias("team_list_item"),
    )
)

result = (
    per_label
    .select(
        "level_filled",
        "team",
        "level",
        "n_team_travel",
        "total_level_filled",
        "stddev_level_filled",
        "team_list_item",
        (F.col("stddev_level_filled") - F.col("level_filled")).alias("stddev_level_filled_minus_level_filled"),
        F.length(F.trim(F.col("team"))).alias("team_len"),
    )
    .orderBy(F.col("n_team_travel").asc_nulls_last())
)
