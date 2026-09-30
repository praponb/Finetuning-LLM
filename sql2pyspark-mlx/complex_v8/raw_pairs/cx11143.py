from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_by_level = Window.partitionBy("team").orderBy(F.col("level").desc(), "id")
w_team_by_level_ties = Window.partitionBy("team").orderBy(F.col("level").desc())

cleaned = (
    df_adjusters
    .filter(F.length(F.trim(F.col("adjuster_name"))) >= 6)
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.substring(F.trim(F.col("adjuster_name")), 1, 4).alias("adjuster_name_prefix"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.min("reported_at").alias("first_claim_reported_at"))
)

profiled = (
    cleaned.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select("s.*", "c.first_claim_reported_at")
)

ranked = (
    profiled
    .select(
        "id",
        "level",
        "adjuster_name_prefix",
        "first_claim_reported_at",
        (F.col("level") - F.lag("level").over(w_team_by_level)).alias("level_change"),
        F.round(F.percent_rank().over(w_team_by_level_ties), 3).alias("level_percent_rank"),
    )
)

result = (
    ranked
    .groupingSets(
        [["level_change", "level"], ["level_change"], []],
        "level_change", "level",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.stddev("level"), 2).alias("stddev_level"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("first_claim_reported_at").alias("first_first_claim_reported_at"),
    )
)
