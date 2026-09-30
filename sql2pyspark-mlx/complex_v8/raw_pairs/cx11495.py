from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_team_len_2_by_level_ti = Window.partitionBy("level", "team_len_2").orderBy(F.col("level").desc())
w_level_team_len_2_by_level_pr = Window.partitionBy("level", "team_len_2").orderBy(F.col("level").desc(), "id").rowsBetween(-6, -1)
w_level_team_len_2_by_level_fu = Window.partitionBy("level", "team_len_2").orderBy(F.col("level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_adjusters
    .filter(F.col("team").isin("Property", "Motor"))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.length(F.trim(F.col("adjuster_name"))).alias("adjuster_name_len"),
    )
)

enriched = (
    prepared
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        "adjuster_name_len",
        F.length(F.trim(F.col("team"))).alias("team_len"),
        F.substring(F.trim(F.col("adjuster_name")), 1, 2).alias("adjuster_name_prefix"),
        F.length(F.trim(F.col("team"))).alias("team_len_2"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.sum("approved_amount").alias("total_claim_approved_amount"))
)

with_counts = (
    enriched.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select("s.*", "c.total_claim_approved_amount")
)

sequenced = (
    with_counts
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        "team_len",
        "adjuster_name_prefix",
        "team_len_2",
        "total_claim_approved_amount",
        F.dense_rank().over(w_level_team_len_2_by_level_ti).alias("level_dense_rank"),
        F.round(F.avg("team_len").over(w_level_team_len_2_by_level_pr), 2).alias("team_len_avg_6_prior"),
        F.first("total_claim_approved_amount").over(w_level_team_len_2_by_level_fu).alias("first_total_claim_approved_amount"),
    )
)

line_level_claims = (
    sequenced.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.id")) & (F.col("c.claimed_amount").between(6745.1, 31715.7)), "inner")
    .select(
        "s.id",
        "s.level",
        "s.team_len",
        "s.adjuster_name_prefix",
        "s.total_claim_approved_amount",
        "s.level_dense_rank",
        "s.first_total_claim_approved_amount",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        "c.claim_date",
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.status").alias("claim_status"),
    )
)

result = (
    line_level_claims
    .select(
        "id",
        "level",
        "team_len",
        "adjuster_name_prefix",
        "total_claim_approved_amount",
        "first_total_claim_approved_amount",
        "claim_id",
        "claim_approved_amount",
        "claim_date",
        "claim_status",
    )
    .orderBy(F.col("total_claim_approved_amount").asc(), "id", "claim_id")
    .limit(10)
)
