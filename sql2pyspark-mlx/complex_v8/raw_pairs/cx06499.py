from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_filled = Window.partitionBy("level_filled")
w_level_filled_team_by_level_f = Window.partitionBy("level_filled", "team").orderBy(F.col("level_filled").desc(), "id")
w_first_claim_date_year_by_lev = Window.partitionBy(F.year("first_claim_date")).orderBy("level_filled", "id").rowsBetween(-1, Window.currentRow)

scoped_adjusters = (
    df_adjusters
    .filter((F.col("adjuster_name").isNotNull()) & (F.col("level").between(1, 3)))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.coalesce(F.col("team"), F.lit("UNKNOWN")).alias("team_or_unknown"),
        F.coalesce(F.col("level"), F.lit(0)).alias("level_filled"),
    )
)

tagged = (
    scoped_adjusters
    .select(
        "id",
        "team",
        "level",
        "team_or_unknown",
        "level_filled",
        F.coalesce(F.col("level_filled"), F.lit(0)).alias("level_filled_filled"),
        F.length(F.trim(F.col("team"))).alias("team_len"),
        F.initcap(F.trim(F.col("team"))).alias("team_clean"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.min("claim_date").alias("first_claim_date"),
        F.min("claimed_amount").alias("min_claim_claimed_amount"),
    )
)

with_stats = (
    tagged.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.first_claim_date",
        "c.min_claim_claimed_amount",
    )
)

metrics = (
    with_stats
    .select(
        "id",
        "team",
        "level",
        "team_or_unknown",
        "level_filled",
        "level_filled_filled",
        "team_len",
        "team_clean",
        "claim_count",
        "first_claim_date",
        "min_claim_claimed_amount",
        F.size(F.collect_set("team").over(w_level_filled)).alias("distinct_team_in_level_filled"),
        F.row_number().over(w_level_filled_team_by_level_f).alias("seq_no"),
        F.max("claim_count").over(w_level_filled).alias("level_filled_max_claim_count"),
        F.round(F.avg("claim_count").over(w_first_claim_date_year_by_lev), 2).alias("claim_count_avg_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "team",
        "level",
        "team_or_unknown",
        "level_filled",
        "level_filled_filled",
        "team_clean",
        "min_claim_claimed_amount",
        "distinct_team_in_level_filled",
        "seq_no",
        "level_filled_max_claim_count",
        "claim_count_avg_2",
    )
)
