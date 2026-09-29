from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claimed_amount_tier_by_repor = Window.partitionBy("claimed_amount_tier").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_claim_date_year_mo = Window.partitionBy("policy_id", F.month("claim_date_year")).orderBy("total_claim_claimed_amount")
w_claimed_amount_tier_by_repor_2 = Window.partitionBy("claimed_amount_tier").orderBy("reported_at", "id")
w_claimed_amount_tier_claim_ty = Window.partitionBy("claimed_amount_tier", "claim_type")

cleaned = (
    df_claims
    .filter(F.col("claimed_amount") > 12726.7)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.when(F.col("claimed_amount") >= 42838.9, "HIGH").when(F.col("claimed_amount") >= 13635.1, "MEDIUM").otherwise("LOW").alias("claimed_amount_tier"),
        F.date_trunc("year", F.col("claim_date")).alias("claim_date_year"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.sum("claimed_amount").alias("total_claim_claimed_amount"))
)

summarized_rows = (
    cleaned.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.total_claim_claimed_amount")
)

windowed = (
    summarized_rows
    .select(
        "*",
        F.first("reported_at", ignorenulls=True).over(w_claimed_amount_tier_by_repor).alias("first_known_reported_at"),
        F.round(F.percent_rank().over(w_policy_id_claim_date_year_mo), 3).alias("total_claim_claimed_amount_percent_rank"),
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_claimed_amount_tier_by_repor_2))) / 60, 1).alias("minutes_since_prev_reported_at"),
        F.max("total_claim_claimed_amount").over(w_claimed_amount_tier_claim_ty).alias("claimed_amount_tier_claim_type_max_total_claim_c"),
        F.sum("total_claim_claimed_amount").over(w_claimed_amount_tier_claim_ty).alias("claimed_amount_tier_claim_type_sum_total_claim_c"),
    )
)

rollup_base = (
    windowed
    .groupBy(F.date_format("claim_date", "yyyy-MM").alias("claim_date_ym"))
    .agg(
        F.sum(F.when(F.col("status").isNotNull(), F.col("claimed_amount"))).alias("claimed_amount_status_is_not_null"),
        F.count(F.when(F.col("status") == "PAID", 1)).alias("n_status_paid"),
        F.percentile_approx("total_claim_claimed_amount_percent_rank", 0.5).alias("median_total_claim_claimed_amount_percent_rank"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.count("*").alias("cnt"),
        F.max("total_claim_claimed_amount").alias("max_total_claim_claimed_amount_check"),
    )
    .filter(F.col("max_total_claim_claimed_amount_check") > 60080.0)
    .drop("max_total_claim_claimed_amount_check")
)

screened = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("median_total_claim_claimed_amount_percent_rank").alias("overall_avg_median_total_claim_claimed_amount_pe")))
    .filter(F.col("median_total_claim_claimed_amount_percent_rank") > F.col("overall_avg_median_total_claim_claimed_amount_pe"))
    .drop("overall_avg_median_total_claim_claimed_amount_pe")
)

labeled = (
    screened
    .select(
        "*",
        F.round(F.col("cnt") / F.nullif(F.col("n_status_paid"), F.lit(0)), 3).alias("cnt_per_n_status_paid"),
        F.least(F.col("cnt"), F.col("n_status_paid")).alias("least_cnt_n_status_paid"),
    )
)

result = (
    labeled
    .select("least_cnt_n_status_paid", "cnt_per_n_status_paid", "claim_date_ym")
    .distinct()
    .orderBy(F.col("claim_date_ym").desc())
)
