from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_count_by_avg_claim_app = Window.partitionBy("claim_count").orderBy(F.col("avg_claim_approved_amount").desc(), "id")
w_team_first_claim_reported_at = Window.partitionBy("team", F.to_date("first_claim_reported_at")).orderBy("first_claim_reported_at", "id")
w_team_first_claim_reported_at_2 = Window.partitionBy("team", F.to_date("first_claim_reported_at")).orderBy("first_claim_reported_at")
w_claim_count = Window.partitionBy("claim_count")
w_level_tier_rn_bucket_by_grea = Window.partitionBy("level_tier", "rn_bucket").orderBy("greatest_max_rn_rn")
w_level_by_stddev_avg_claim_ap = Window.partitionBy("level").orderBy("stddev_avg_claim_approved_amount_change")
w_level_tier_rn_bucket_by_grea_2 = Window.partitionBy("level_tier", "rn_bucket").orderBy("greatest_max_rn_rn", "claim_count", "level", "rn").rowsBetween(-3, -1)
w_level_tier_rn_bucket_by_grea_3 = Window.partitionBy("level_tier", "rn_bucket").orderBy("greatest_max_rn_rn", "claim_count", "level", "rn").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_level_tier_rn_bucket_by_grea_4 = Window.partitionBy("level_tier", "rn_bucket").orderBy("greatest_max_rn_rn", "claim_count", "level", "rn")

filtered = (
    df_adjusters
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.when(F.col("level") >= 3, "HIGH").when(F.col("level") >= 2, "MEDIUM").otherwise("LOW").alias("level_tier"),
        F.concat_ws("-", F.col("adjuster_name"), F.col("team")).alias("adjuster_name_team_label"),
        F.initcap(F.trim(F.col("adjuster_name"))).alias("adjuster_name_clean"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.min("reported_at").alias("first_claim_reported_at"),
        F.round(F.avg("approved_amount"), 2).alias("avg_claim_approved_amount"),
    )
)

summarized_rows = (
    filtered.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.first_claim_reported_at",
        "c.avg_claim_approved_amount",
    )
)

with_trends = (
    summarized_rows
    .select(
        "*",
        (F.col("avg_claim_approved_amount") - F.lag("avg_claim_approved_amount").over(w_claim_count_by_avg_claim_app)).alias("avg_claim_approved_amount_change"),
        F.row_number().over(w_team_first_claim_reported_at).alias("rn"),
        F.round(F.cume_dist().over(w_team_first_claim_reported_at_2), 3).alias("first_claim_reported_at_cume_dist"),
    )
)

totals = (
    with_trends
    .filter(~F.col("team").isin("Property", "Health"))
    .groupBy("claim_count", "level", "rn")
    .agg(
        F.round(F.stddev("avg_claim_approved_amount_change"), 2).alias("stddev_avg_claim_approved_amount_change"),
        F.max("rn").alias("max_rn"),
        F.sum("claim_count").alias("sum_claim_count_check"),
    )
    .filter(F.col("sum_claim_count_check") > 2)
    .drop("sum_claim_count_check")
)

annotated = (
    totals
    .filter(F.col("claim_count").between(0, 6))
    .select(
        "claim_count",
        "level",
        "rn",
        "stddev_avg_claim_approved_amount_change",
        "max_rn",
        (F.floor(F.col("rn") / 2) * 2).alias("rn_bucket"),
        F.when(F.col("level") >= 3, "large").when(F.col("level") >= 2, "medium").otherwise("small").alias("level_tier"),
        F.greatest(F.col("max_rn"), F.col("rn")).alias("greatest_max_rn_rn"),
    )
)

metrics = (
    annotated
    .select(
        "*",
        F.count(F.lit(1)).over(w_claim_count).alias("rows_in_claim_count"),
        F.dense_rank().over(w_level_tier_rn_bucket_by_grea).alias("greatest_max_rn_rn_dense_rank"),
        F.rank().over(w_level_by_stddev_avg_claim_ap).alias("stddev_avg_claim_approved_amount_change_rank"),
        F.round(F.avg("greatest_max_rn_rn").over(w_level_tier_rn_bucket_by_grea_2), 2).alias("greatest_max_rn_rn_avg_3_prior"),
        F.first("rn_bucket", ignorenulls=True).over(w_level_tier_rn_bucket_by_grea_3).alias("first_known_rn_bucket"),
        F.lag("rn_bucket", 1, 0).over(w_level_tier_rn_bucket_by_grea_4).alias("prev_rn_bucket"),
    )
)

result = (
    metrics
    .select(
        "claim_count",
        "level",
        "rn_bucket",
        "level_tier",
        "greatest_max_rn_rn",
        "rows_in_claim_count",
        "stddev_avg_claim_approved_amount_change_rank",
        "greatest_max_rn_rn_avg_3_prior",
        "first_known_rn_bucket",
        "prev_rn_bucket",
    )
)
