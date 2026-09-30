from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("team", "team_group").orderBy(F.col("level_distance").desc(), F.col("id").desc())
w_claim_claim_type_count = Window.partitionBy("claim_claim_type_count")
w_team_group_level_by_level_di = Window.partitionBy("team_group", "level").orderBy(F.col("level_distance").desc(), "id").rowsBetween(-3, -1)
w_team_group_level_by_level_di_2 = Window.partitionBy("team_group", "level").orderBy(F.col("level_distance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_adjusters
    .filter(F.upper(F.trim(F.col("adjuster_name"))).like("A%"))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.lower(F.trim(F.col("adjuster_name"))).alias("adjuster_name_clean"),
        F.abs(F.col("level") - 3).alias("level_distance"),
        F.when(F.col("team").isin("Health", "Property"), "group_a").when(F.col("team") == "Travel", "group_b").otherwise("other").alias("team_group"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count(F.when(F.col("claim_type") == "fire", 1)).alias("claim_claim_type_count"),
        F.round(F.avg("approved_amount"), 2).alias("avg_claim_approved_amount"),
    )
)

profiled = (
    recent.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
        "c.avg_claim_approved_amount",
    )
)

latest = (
    profiled
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_trends = (
    latest
    .select(
        "*",
        F.sum("claim_claim_type_count").over(w_claim_claim_type_count).alias("claim_claim_type_count_sum_claim_claim_type_coun"),
        F.round(F.avg("avg_claim_approved_amount").over(w_team_group_level_by_level_di), 2).alias("avg_claim_approved_amount_avg_3_prior"),
        F.count(F.lit(1)).over(w_claim_claim_type_count).alias("rows_in_claim_claim_type_count"),
        F.first("avg_claim_approved_amount").over(w_team_group_level_by_level_di_2).alias("first_avg_claim_approved_amount"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        "adjuster_name_clean",
        "team_group",
        "avg_claim_approved_amount",
        "claim_claim_type_count_sum_claim_claim_type_coun",
        "rows_in_claim_claim_type_count",
        "first_avg_claim_approved_amount",
        F.when(F.col("level") >= 3, "HIGH").when(F.col("level") >= 2, "MEDIUM").otherwise("LOW").alias("level_tier"),
        F.abs(F.col("claim_claim_type_count") - 2).alias("claim_claim_type_count_distance"),
    )
)
