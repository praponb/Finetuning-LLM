from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_group_team = Window.partitionBy("team_group", "team")
w_team_or_unknown_by_level = Window.partitionBy("team_or_unknown").orderBy(F.col("level").desc(), "id")
w_team_or_unknown_by_level_pri = Window.partitionBy("team_or_unknown").orderBy(F.col("level").desc(), "id").rowsBetween(-1, -1)
w_team_or_unknown_by_level_ful = Window.partitionBy("team_or_unknown").orderBy("level", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_team_last_claim_date_2_year = Window.partitionBy("team", F.year("last_claim_date_2"))
w_team_or_unknown_last_level_b = Window.partitionBy("team_or_unknown", "last_level").orderBy("last_level", "id")
w_last_claim_date_year_by_team = Window.partitionBy(F.year("last_claim_date")).orderBy("team_group_team_max_level", "id")
w_last_claim_date_year_by_team_2 = Window.partitionBy(F.year("last_claim_date")).orderBy("team_group_team_max_level")
w_latest = Window.partitionBy("team_group").orderBy("team_group_team_max_level_quartile", "id")

prepared_adjusters = (
    df_adjusters
    .filter((F.upper(F.trim(F.col("adjuster_name"))).like("G%")) | (F.col("level") >= 2))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.substring(F.trim(F.col("team")), 1, 3).alias("team_prefix"),
        F.when(F.col("team").isin("Motor", "Health"), "group_a").when(F.col("team") == "Property", "group_b").otherwise("other").alias("team_group"),
        F.coalesce(F.col("team"), F.lit("UNKNOWN")).alias("team_or_unknown"),
    )
)

filtered = (
    prepared_adjusters.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.id")) & (F.col("c.approved_amount").between(0.0, 33690.0)) & (F.col("c.claimed_amount") < 40639.7), "left_anti")
)

windowed = (
    filtered
    .select(
        "*",
        F.max("level").over(w_team_group_team).alias("team_group_team_max_level"),
        F.row_number().over(w_team_or_unknown_by_level).alias("seq_no"),
        F.round(F.avg("level").over(w_team_or_unknown_by_level_pri), 2).alias("level_avg_1_prior"),
        F.last("level").over(w_team_or_unknown_by_level_ful).alias("last_level"),
        F.round(F.col("level") / F.nullif(F.sum("level").over(w_team_group_team), F.lit(0)), 4).alias("level_share_of_team_group_team"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.max("claim_date").alias("last_claim_date"),
        F.max("claim_date").alias("last_claim_date_2"),
    )
)

summarized_rows = (
    windowed.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select("s.*", "c.last_claim_date", "c.last_claim_date_2")
)

claim_stats_2 = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count(F.when(F.col("claim_type") == "medical", 1)).alias("claim_claim_type_count"),
    )
)

with_stats = (
    summarized_rows.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

ranked = (
    with_stats
    .select(
        "*",
        F.size(F.collect_set("team_group").over(w_team_last_claim_date_2_year)).alias("distinct_team_group_in_team_last_claim_date_2_ye"),
        F.row_number().over(w_team_or_unknown_last_level_b).alias("seq_no_2"),
        F.min("level").over(w_team_last_claim_date_2_year).alias("team_last_claim_date_2_year_min_level"),
        F.ntile(4).over(w_last_claim_date_year_by_team).alias("team_group_team_max_level_quartile"),
        F.round(F.cume_dist().over(w_last_claim_date_year_by_team_2), 3).alias("team_group_team_max_level_cume_dist"),
    )
)

deduped = (
    ranked
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .select(
        "id",
        "adjuster_name",
        "team_prefix",
        "team_or_unknown",
        "team_group_team_max_level",
        "seq_no",
        "level_avg_1_prior",
        "last_level",
        "last_claim_date",
        "claim_claim_type_count",
        "distinct_team_group_in_team_last_claim_date_2_ye",
        "seq_no_2",
        "team_group_team_max_level_quartile",
        "team_group_team_max_level_cume_dist",
        (F.floor(F.col("seq_no") / 2) * 2).alias("seq_no_bucket"),
        F.datediff(F.col("last_claim_date_2"), F.col("last_claim_date")).alias("days_last_claim_date_to_last_claim_date_2"),
    )
    .orderBy(F.col("claim_claim_type_count").asc_nulls_last(), F.col("id").desc())
)
