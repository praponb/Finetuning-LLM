from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_adjuster_id_by_report = Window.partitionBy("status", "adjuster_id").orderBy(F.col("reported_at_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_adjuster_id_by_report_2 = Window.partitionBy("status", "adjuster_id").orderBy(F.col("reported_at_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_first_claim_date_plus_30d_mo = Window.partitionBy(F.month("first_claim_date_plus_30d")).orderBy("approved_amount", "id")
w_status_policy_id = Window.partitionBy("status", "policy_id")
w_all_by_approved_amount_dista = Window.orderBy(F.col("approved_amount_distance_minus_avg").desc(), "id").rowsBetween(-2, -1)

base_claims = (
    df_claims
    .filter(F.col("status").isin("PAID", "IN_REVIEW", "OPEN"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.when(F.col("status").isin("REJECTED", "OPEN"), "group_a").when(F.col("status") == "IN_REVIEW", "group_b").otherwise("other").alias("status_group"),
        F.abs(F.col("approved_amount") - 9389.3).alias("approved_amount_distance"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.min("claim_date").alias("first_claim_date"),
        F.max("claimed_amount").alias("max_claim_claimed_amount"),
    )
)

profiled = (
    base_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "c.first_claim_date", "c.max_claim_claimed_amount")
)

policy_id_stats = (
    profiled
    .groupBy("policy_id")
    .agg(F.avg("approved_amount_distance").alias("avg_approved_amount_distance"))
)

above_average = (
    profiled.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.approved_amount_distance") > F.col("g.avg_approved_amount_distance"))
    .select(
        "s.*",
        "g.avg_approved_amount_distance",
        F.round(F.col("s.approved_amount_distance") - F.col("g.avg_approved_amount_distance"), 2).alias("approved_amount_distance_minus_avg"),
    )
)

tagged = (
    above_average
    .filter(F.col("avg_approved_amount_distance") > 17500.0)
    .select(
        "*",
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
        F.date_add(F.col("first_claim_date"), 30).alias("first_claim_date_plus_30d"),
        F.lower(F.regexp_replace(F.trim(F.col("status_group")), " ", "_")).alias("status_group_slug"),
    )
)

sequenced = (
    tagged
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
        "approved_amount_distance",
        "first_claim_date",
        "approved_amount_distance_minus_avg",
        "reported_at_date",
        "first_claim_date_plus_30d",
        F.nth_value("claimed_amount", 2).over(w_status_adjuster_id_by_report).alias("second_claimed_amount"),
        F.first("max_claim_claimed_amount", ignorenulls=True).over(w_status_adjuster_id_by_report_2).alias("first_known_max_claim_claimed_amount"),
        F.row_number().over(w_first_claim_date_plus_30d_mo).alias("rn"),
        F.max("claimed_amount").over(w_status_policy_id).alias("status_policy_id_max_claimed_amount"),
        F.round(F.avg("max_claim_claimed_amount").over(w_all_by_approved_amount_dista), 2).alias("max_claim_claimed_amount_avg_2_prior"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "reported_at_date",
        "second_claimed_amount",
        "first_known_max_claim_claimed_amount",
        "rn",
        "status_policy_id_max_claimed_amount",
        "max_claim_claimed_amount_avg_2_prior",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.abs(F.col("rn") - 4).alias("rn_distance"),
    )
    .orderBy("status_or_unknown")
)
