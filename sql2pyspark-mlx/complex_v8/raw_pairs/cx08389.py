from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_claim_count = Window.partitionBy("level", "claim_count")
w_all_by_adjuster_name_len = Window.orderBy("adjuster_name_len", "id")
w_all_by_adjuster_name_len_tie = Window.orderBy("adjuster_name_len")

valid_adjusters = (
    df_adjusters
    .filter((F.col("level") >= 3) & (F.col("team") == "Property"))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.concat_ws(" / ", F.col("team"), F.col("adjuster_name")).alias("team_adjuster_name_label"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.max("reported_at").alias("last_claim_reported_at"))
)

summarized_rows = (
    valid_adjusters.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select("s.*", "c.last_claim_reported_at")
)

screened = (
    summarized_rows.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.id")) & (F.col("c.claim_type") == "fire") & (F.col("c.approved_amount") <= 14993.0), "left_semi")
)

labeled = (
    screened
    .select(
        "*",
        F.abs(F.col("level") - 3).alias("level_distance"),
        F.concat_ws("-", F.col("adjuster_name"), F.col("team")).alias("adjuster_name_team_label"),
        F.length(F.trim(F.col("adjuster_name"))).alias("adjuster_name_len"),
    )
)

claim_stats_2 = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.count("*").alias("claim_count"))
)

with_stats = (
    labeled.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

with_trends = (
    with_stats
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        "team_adjuster_name_label",
        "last_claim_reported_at",
        "level_distance",
        "adjuster_name_len",
        "claim_count",
        F.round(F.col("adjuster_name_len") / F.nullif(F.sum("adjuster_name_len").over(w_level_claim_count), F.lit(0)), 4).alias("adjuster_name_len_share_of_level_claim_count"),
        F.lead("level", 3).over(w_all_by_adjuster_name_len).alias("next3_level"),
        F.round(F.percent_rank().over(w_all_by_adjuster_name_len_tie), 3).alias("adjuster_name_len_percent_rank"),
        F.sum("level_distance").over(w_level_claim_count).alias("level_claim_count_sum_level_distance"),
        F.round(F.col("claim_count") / F.nullif(F.sum("claim_count").over(w_level_claim_count), F.lit(0)), 4).alias("claim_count_share_of_level_claim_count"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "adjuster_name_len_share_of_level_claim_count",
        "adjuster_name_len_percent_rank",
    )
    .unpivot(
        ids=["id"],
        values=["adjuster_name_len_share_of_level_claim_count", "adjuster_name_len_percent_rank"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "metric_name")
)
