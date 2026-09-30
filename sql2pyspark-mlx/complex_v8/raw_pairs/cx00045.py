from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claimed_amount_full = Window.orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_date_year_to_date = Window.partitionBy(F.to_date("claim_date_year"))
w_all_by_claimed_amount = Window.orderBy("claimed_amount", "id")
w_policy_id_by_rows_in_claim_d = Window.partitionBy("policy_id").orderBy(F.col("rows_in_claim_date_year_to_date").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_claim_count_claim_date_year = Window.partitionBy("claim_count", F.date_trunc("month", "claim_date_year")).orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(-1, -1)
w_adjuster_id_policy_id = Window.partitionBy("adjuster_id", "policy_id")
w_claim_count_claim_date_year_2 = Window.partitionBy("claim_count", F.date_trunc("month", "claim_date_year")).orderBy(F.col("claimed_amount").desc(), "id")

base = (
    df_claims
    .filter(F.hour(F.col("reported_at")).between(8, 10))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.date_trunc("year", F.col("claim_date")).alias("claim_date_year"),
    )
)

windowed = (
    base
    .select(
        "*",
        F.nth_value("approved_amount", 2).over(w_all_by_claimed_amount_full).alias("second_approved_amount"),
        F.count(F.lit(1)).over(w_claim_date_year_to_date).alias("rows_in_claim_date_year_to_date"),
        (F.col("approved_amount") - F.lag("approved_amount").over(w_all_by_claimed_amount)).alias("approved_amount_change"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.count("*").alias("claim_count"))
)

profiled = (
    windowed.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

sequenced = (
    profiled
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "status",
        "rows_in_claim_date_year_to_date",
        "approved_amount_change",
        "claim_count",
        F.sum("second_approved_amount").over(w_policy_id_by_rows_in_claim_d).alias("second_approved_amount_sum_2_prior"),
        F.max("claimed_amount").over(w_claim_count_claim_date_year).alias("claimed_amount_max_1_prior"),
        F.size(F.collect_set("claim_type").over(w_adjuster_id_policy_id)).alias("distinct_claim_type_in_adjuster_id_policy_id"),
        F.size(F.collect_set("status").over(w_adjuster_id_policy_id)).alias("distinct_status_in_adjuster_id_policy_id"),
        F.ntile(4).over(w_claim_count_claim_date_year_2).alias("claimed_amount_quartile"),
    )
)

claim_stats_2 = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.sum("approved_amount").alias("total_claim_approved_amount"),
        F.max("claim_date").alias("last_claim_date"),
        F.count("*").alias("claim_count_2"),
    )
)

summarized_rows = (
    sequenced.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        "c.total_claim_approved_amount",
        "c.last_claim_date",
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
    )
)

with_details_adjuster = (
    summarized_rows.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.status",
        "s.approved_amount_change",
        "s.second_approved_amount_sum_2_prior",
        "s.claimed_amount_max_1_prior",
        "s.distinct_claim_type_in_adjuster_id_policy_id",
        "s.distinct_status_in_adjuster_id_policy_id",
        "s.claimed_amount_quartile",
        "s.last_claim_date",
        "s.claim_count_2",
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
    )
)

tagged = (
    with_details_adjuster
    .select(
        "id",
        "status",
        "second_approved_amount_sum_2_prior",
        "claimed_amount_max_1_prior",
        "distinct_claim_type_in_adjuster_id_policy_id",
        "claimed_amount_quartile",
        "last_claim_date",
        F.lower(F.trim(F.col("adjuster_name"))).alias("adjuster_name_clean"),
        F.date_format(F.col("last_claim_date"), "MMM yyyy").alias("last_claim_date_label"),
        F.length(F.trim(F.col("adjuster_team"))).alias("adjuster_team_len"),
    )
)

result = (
    tagged
    .select(
        "id",
        "status",
        "second_approved_amount_sum_2_prior",
        "claimed_amount_max_1_prior",
        "distinct_claim_type_in_adjuster_id_policy_id",
        "claimed_amount_quartile",
        "last_claim_date",
        "adjuster_name_clean",
        "last_claim_date_label",
        "adjuster_team_len",
        F.substring(F.trim(F.col("last_claim_date_label")), 1, 4).alias("last_claim_date_label_prefix"),
    )
)
