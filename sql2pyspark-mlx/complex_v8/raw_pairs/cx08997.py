from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_claim_date_month = Window.partitionBy("policy_id", F.date_trunc("month", "claim_date"))
w_policy_id_by_reported_at_dat = Window.partitionBy("policy_id").orderBy(F.col("reported_at_date").asc_nulls_last(), "id")
w_policy_id_by_reported_at_dat_2 = Window.partitionBy("policy_id").orderBy(F.col("reported_at_date").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_adjuster_id_reported_at_year = Window.partitionBy("adjuster_id", F.year("reported_at")).orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_row_count_by_max_claimed_amo = Window.partitionBy("row_count").orderBy("max_claimed_amount", "claim_id", "claim_date_ym", "claim_status").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_max_claimed_amount_ru = Window.orderBy(F.col("max_claimed_amount").desc(), "claim_id", "claim_date_ym", "claim_status").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_row_count = Window.partitionBy("row_count")
w_row_count_by_max_claimed_amo_2 = Window.partitionBy("row_count").orderBy("max_claimed_amount", "claim_id", "claim_date_ym", "claim_status")
w_all_by_max_claimed_amount = Window.orderBy(F.col("max_claimed_amount").desc(), "claim_id", "claim_date_ym", "claim_status")

valid_claims = (
    df_claims
    .filter((F.col("claim_type").isNotNull()) & (F.col("policy_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
    )
)

with_trends = (
    valid_claims
    .select(
        "*",
        F.size(F.collect_set("claim_type").over(w_policy_id_claim_date_month)).alias("distinct_claim_type_in_policy_id_claim_date_mont"),
        F.row_number().over(w_policy_id_by_reported_at_dat).alias("reported_at_date_seq"),
        F.round(F.avg("approved_amount").over(w_policy_id_by_reported_at_dat_2), 2).alias("approved_amount_avg_6_prior"),
        F.sum("claimed_amount").over(w_policy_id_claim_date_month).alias("policy_id_claim_date_month_sum_claimed_amount"),
        F.max("approved_amount").over(w_adjuster_id_reported_at_year).alias("max_approved_amount_so_far"),
        F.count(F.lit(1)).over(w_policy_id_claim_date_month).alias("rows_in_policy_id_claim_date_month"),
    )
)

line_level = (
    with_trends.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.status") != "IN_REVIEW"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.distinct_claim_type_in_policy_id_claim_date_mont",
        "s.approved_amount_avg_6_prior",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.status").alias("claim_status"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

rollup_base = (
    line_level
    .groupBy(
        "claim_id",
        F.date_format("claim_date", "yyyy-MM").alias("claim_date_ym"),
        "claim_status",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.stddev("approved_amount_avg_6_prior"), 2).alias("stddev_approved_amount_avg_6_prior"),
        F.sum("distinct_claim_type_in_policy_id_claim_date_mont").alias("total_distinct_claim_type_in_policy_id_claim_dat"),
        F.countDistinct("claim_id").alias("distinct_claim_ids"),
        F.max("claimed_amount").alias("max_claimed_amount"),
        F.max("claim_date").alias("last_claim_date"),
    )
    .filter(F.col("row_count") >= 2)
)

sequenced = (
    rollup_base
    .select(
        "claim_id",
        "claim_date_ym",
        "claim_status",
        "row_count",
        "distinct_claim_ids",
        "max_claimed_amount",
        "last_claim_date",
        F.last("row_count").over(w_row_count_by_max_claimed_amo).alias("last_row_count"),
        F.first("max_claimed_amount", ignorenulls=True).over(w_all_by_max_claimed_amount_ru).alias("first_known_max_claimed_amount"),
        F.min("row_count").over(w_row_count).alias("row_count_min_row_count"),
        F.lead("distinct_claim_ids", 1, 0).over(w_row_count_by_max_claimed_amo_2).alias("next_distinct_claim_ids"),
        F.lead("row_count").over(w_all_by_max_claimed_amount).alias("next_row_count"),
        F.round(F.col("row_count") / F.nullif(F.sum("row_count").over(w_row_count), F.lit(0)), 4).alias("row_count_share_of_row_count"),
    )
)

tagged = (
    sequenced
    .filter(F.col("last_row_count") < 10)
    .select(
        "*",
        F.substring(F.trim(F.col("claim_date_ym")), 1, 4).alias("claim_date_ym_prefix"),
        F.abs(F.col("last_row_count") - 7).alias("last_row_count_distance"),
    )
)

result = (
    tagged
    .select(
        "claim_id",
        "claim_status",
        "row_count",
        "last_claim_date",
        "row_count_min_row_count",
        "claim_date_ym_prefix",
        "last_row_count_distance",
        F.substring(F.trim(F.col("claim_date_ym")), 1, 4).alias("claim_date_ym_prefix_2"),
        F.round(F.col("next_row_count") / F.nullif(F.col("distinct_claim_ids"), F.lit(0)), 3).alias("next_row_count_per_distinct_claim_ids"),
    )
)
