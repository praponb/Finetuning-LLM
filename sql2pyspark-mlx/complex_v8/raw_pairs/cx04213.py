from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_by_level_prior_6 = Window.partitionBy("team").orderBy(F.col("level").desc_nulls_first(), "id").rowsBetween(-6, -1)
w_team_by_level_running = Window.partitionBy("team").orderBy(F.col("level").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_team_level = Window.partitionBy("team", "level")
w_team_by_level_full = Window.partitionBy("team").orderBy(F.col("level").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_adjusters = (
    df_adjusters
    .filter(F.col("team") != "Travel")
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.substring(F.trim(F.col("team")), 1, 3).alias("team_prefix"),
    )
)

enriched = (
    filtered_adjusters
    .withColumn("team_prefix_prefix", F.substring(F.trim(F.col("team_prefix")), 1, 4))
)

metrics = (
    enriched
    .select(
        "id",
        "adjuster_name",
        "level",
        "team_prefix",
        "team_prefix_prefix",
        F.round(F.avg("level").over(w_team_by_level_prior_6), 2).alias("level_avg_6_prior"),
        F.first("level", ignorenulls=True).over(w_team_by_level_running).alias("first_known_level"),
        F.nth_value("level", 2).over(w_team_by_level_full).alias("second_level"),
        F.count(F.lit(1)).over(w_team_level).alias("rows_in_team_level"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.max("claimed_amount").alias("max_claim_claimed_amount"))
)

profiled = (
    metrics.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select("s.*", "c.max_claim_claimed_amount")
)

result = (
    profiled
    .groupingSets(
        [["second_level", "first_known_level", "level"], ["second_level"], []],
        "second_level", "first_known_level", "level",
    )
    .agg(F.min("level").alias("min_level"))
    .orderBy(
        F.col("second_level").asc_nulls_last(),
        F.col("first_known_level").asc_nulls_last(),
        F.col("level").asc_nulls_last(),
    )
)
