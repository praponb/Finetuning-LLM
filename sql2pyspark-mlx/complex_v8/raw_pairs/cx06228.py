from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id = Window.partitionBy("adjuster_id")
w_first_claim_reported_at_year = Window.partitionBy(F.year("first_claim_reported_at")).orderBy("total_claimed_amount", "adjuster_id")
w_first_claim_reported_at_year_2 = Window.partitionBy(F.year("first_claim_reported_at")).orderBy("total_claimed_amount", "adjuster_id").rowsBetween(-6, -1)
w_first_claim_reported_at_to_d = Window.partitionBy(F.to_date("first_claim_reported_at"), "adjuster_id").orderBy("first_claim_reported_at", "adjuster_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_first_claim_reported_at_year_3 = Window.partitionBy(F.year("first_claim_reported_at")).orderBy("total_claimed_amount", "adjuster_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.concat_ws(" / ", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
        F.when(F.col("status").isin("OPEN", "REJECTED"), "group_a").when(F.col("status") == "PAID", "group_b").otherwise("other").alias("status_group"),
    )
)

exploded_detail = (
    valid.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status_group",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

per_group = (
    exploded_detail
    .filter(F.col("approved_amount") >= 2569.2)
    .groupBy("adjuster_id")
    .agg(
        F.percentile_approx("approved_amount", 0.5).alias("median_approved_amount"),
        F.sum("claimed_amount").alias("total_claimed_amount"),
        F.min("claim_reported_at").alias("first_claim_reported_at"),
        F.count("*").alias("row_count"),
        F.round(F.stddev("approved_amount"), 2).alias("stddev_approved_amount"),
    )
    .filter(F.col("row_count") >= 3)
)

joined_adjuster = (
    per_group.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.adjuster_id",
        "s.median_approved_amount",
        "s.total_claimed_amount",
        "s.first_claim_reported_at",
        "a.adjuster_name",
    )
)

ranked = (
    joined_adjuster
    .select(
        "adjuster_id",
        "total_claimed_amount",
        "first_claim_reported_at",
        "adjuster_name",
        F.lag("median_approved_amount").over(w_first_claim_reported_at_year).alias("prev_median_approved_amount"),
        F.round(F.avg("median_approved_amount").over(w_first_claim_reported_at_year_2), 2).alias("median_approved_amount_avg_6_prior"),
        F.min("median_approved_amount").over(w_first_claim_reported_at_to_d).alias("min_median_approved_amount_so_far"),
        F.count(F.lit(1)).over(w_adjuster_id).alias("rows_in_adjuster_id"),
        F.round(F.avg("median_approved_amount").over(w_adjuster_id), 2).alias("adjuster_id_avg_median_approved_amount"),
        F.nth_value("median_approved_amount", 2).over(w_first_claim_reported_at_year_3).alias("second_median_approved_amount"),
    )
)

result = (
    ranked
    .select(
        "adjuster_id",
        "total_claimed_amount",
        "first_claim_reported_at",
        "adjuster_name",
        "prev_median_approved_amount",
        "median_approved_amount_avg_6_prior",
        "min_median_approved_amount_so_far",
        "second_median_approved_amount",
        F.round(F.col("rows_in_adjuster_id") / F.nullif(F.col("total_claimed_amount"), F.lit(0)), 3).alias("rows_in_adjuster_id_per_total_claimed_amount"),
        F.date_format(F.col("first_claim_reported_at"), "yyyy-MM").alias("first_claim_reported_at_label"),
    )
    .orderBy("median_approved_amount_avg_6_prior")
)
