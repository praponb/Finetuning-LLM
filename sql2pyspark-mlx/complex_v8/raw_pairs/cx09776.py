from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_reported_at_full = Window.partitionBy("status").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_reported_at = Window.orderBy("reported_at", "id")
w_status_by_reported_at_runnin = Window.partitionBy("status").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_reported_at = Window.partitionBy("status").orderBy("reported_at", "id")
w_all_by_min_claim_claimed_amo = Window.orderBy(F.col("min_claim_claimed_amount").asc_nulls_last(), "rn").rowsBetween(-6, -1)
w_rn = Window.partitionBy("rn")

valid = (
    df_claims
    .filter((F.col("status") == "PAID") | (F.col("adjuster_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "status",
        "reported_at",
    )
)

with_windows = (
    valid
    .select(
        "id",
        "policy_id",
        "claim_type",
        "status",
        F.nth_value("reported_at", 2).over(w_status_by_reported_at_full).alias("second_reported_at"),
        F.row_number().over(w_all_by_reported_at).alias("rn"),
        F.first("reported_at", ignorenulls=True).over(w_status_by_reported_at_runnin).alias("first_known_reported_at"),
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_status_by_reported_at))) / 60, 1).alias("minutes_since_prev_reported_at"),
        F.round(F.cume_dist().over(w_status_by_reported_at), 3).alias("reported_at_cume_dist"),
        F.lead("reported_at", 2).over(w_all_by_reported_at).alias("next2_reported_at"),
    )
)

exploded_detail_claims = (
    with_windows.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claim_date") < F.lit("2025-05-26").cast("date")), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_type",
        "s.second_reported_at",
        "s.rn",
        "s.first_known_reported_at",
        "s.reported_at_cume_dist",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        "c.claim_date",
    )
)

claim_type_stats = (
    exploded_detail_claims
    .groupBy("claim_type")
    .agg(F.avg("reported_at_cume_dist").alias("avg_reported_at_cume_dist"))
)

outliers = (
    exploded_detail_claims.alias("s")
    .join(claim_type_stats.alias("g"), F.col("g.claim_type") == F.col("s.claim_type"), "inner")
    .filter(F.col("s.reported_at_cume_dist") > F.col("g.avg_reported_at_cume_dist"))
    .select(
        "s.*",
        "g.avg_reported_at_cume_dist",
        F.round(F.col("s.reported_at_cume_dist") - F.col("g.avg_reported_at_cume_dist"), 2).alias("reported_at_cume_dist_minus_avg"),
    )
)

grouped = (
    outliers
    .filter(F.col("claim_id").isNotNull())
    .groupBy("rn")
    .agg(
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.min("claim_claimed_amount").alias("min_claim_claimed_amount"),
        F.sum("reported_at_cume_dist_minus_avg").alias("total_reported_at_cume_dist_minus_avg"),
        F.sum("claim_claimed_amount").alias("total_claim_claimed_amount"),
        F.round(F.sum("avg_reported_at_cume_dist"), 2).alias("total_avg_reported_at_cume_dist"),
        F.avg("claim_claimed_amount").alias("avg_claim_claimed_amount_check"),
    )
    .filter(F.col("avg_claim_claimed_amount_check") > 3703.0)
    .drop("avg_claim_claimed_amount_check")
)

metrics = (
    grouped
    .withColumn("rn_avg_6_prior", F.round(F.avg("rn").over(w_all_by_min_claim_claimed_amo), 2))
    .withColumn("min_claim_claimed_amount_vs_rn_avg", F.round(F.col("min_claim_claimed_amount") - F.avg("min_claim_claimed_amount").over(w_rn), 2))
)

result = (
    metrics
    .filter(F.col("total_avg_reported_at_cume_dist").between(1.8, 4.2))
    .select(
        "rn",
        "total_claim_claimed_amount",
        F.least(F.col("min_claim_claimed_amount"), F.col("rn_avg_6_prior")).alias("least_min_claim_claimed_amount_rn_avg_6_prior"),
        (F.col("rn") + F.col("rn_avg_6_prior")).alias("rn_plus_rn_avg_6_prior"),
    )
    .orderBy("least_min_claim_claimed_amount_rn_avg_6_prior")
)
