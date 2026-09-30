from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_count_2_by_approved_am = Window.partitionBy("claim_count_2").orderBy("approved_amount_filled", "id")
w_adjuster_id = Window.partitionBy("adjuster_id")
w_claim_date_year_policy_id_by = Window.partitionBy(F.year("claim_date"), "policy_id").orderBy("approved_amount_filled", "id")
w_reported_at_day_month_policy = Window.partitionBy(F.date_trunc("month", "reported_at_day"), "policy_id")
w_all_by_claim_count = Window.orderBy("claim_count", "id")
w_reported_at_day_month_claim = Window.partitionBy(F.date_trunc("month", "reported_at_day"), "claim_type_group")
w_all_by_claim_count_full = Window.orderBy("claim_count", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claim_count_ties = Window.orderBy("claim_count")

base = (
    df_claims
    .filter((F.col("approved_amount") > 4868.7) & (F.col("claimed_amount") > 17759.3))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        F.date_trunc("day", F.col("reported_at")).alias("reported_at_day"),
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
        F.when(F.col("claim_type").isin("medical", "collision"), "group_a").when(F.col("claim_type") == "fire", "group_b").otherwise("other").alias("claim_type_group"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.count("*").alias("claim_count"), F.count("*").alias("claim_count_2"))
)

with_stats = (
    base.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
    )
)

sequenced = (
    with_stats
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "reported_at_day",
        "claim_type_group",
        "claim_count",
        F.ntile(4).over(w_claim_count_2_by_approved_am).alias("approved_amount_filled_quartile"),
        F.round(F.col("claim_count") / F.nullif(F.sum("claim_count").over(w_adjuster_id), F.lit(0)), 4).alias("claim_count_share_of_adjuster_id"),
        F.lag("reported_at_day").over(w_claim_date_year_policy_id_by).alias("prev_reported_at_day"),
    )
)

derived = (
    sequenced
    .filter(F.col("claim_count_share_of_adjuster_id") < 0.7)
    .select(
        "*",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("prev_reported_at_day")).alias("days_since_prev_reported_at_day"),
        F.date_trunc("day", F.col("reported_at_day")).alias("reported_at_day_day"),
        F.concat(F.col("claim_type_group"), F.lit("#"), F.col("id").cast("string")).alias("claim_type_group_ref"),
    )
)

ranked = (
    derived
    .select(
        "*",
        F.max("claim_count").over(w_reported_at_day_month_policy).alias("reported_at_day_month_policy_id_max_claim_count"),
        F.size(F.collect_set("claim_type").over(w_reported_at_day_month_policy)).alias("distinct_claim_type_in_reported_at_day_month_pol"),
        F.round(F.col("claim_count") / F.nullif(F.sum("claim_count").over(w_reported_at_day_month_policy), F.lit(0)), 4).alias("claim_count_share_of_reported_at_day_month_polic"),
        F.sum("claim_count").over(w_reported_at_day_month_policy).alias("reported_at_day_month_policy_id_sum_claim_count"),
    )
)

with_windows = (
    ranked
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "reported_at_day",
        "claim_type_group",
        "approved_amount_filled_quartile",
        "claim_count_share_of_adjuster_id",
        "days_since_prev_reported_at_day",
        "reported_at_day_day",
        "claim_type_group_ref",
        "reported_at_day_month_policy_id_max_claim_count",
        "distinct_claim_type_in_reported_at_day_month_pol",
        "reported_at_day_month_policy_id_sum_claim_count",
        F.round((F.unix_timestamp("reported_at_day") - F.unix_timestamp(F.lag("reported_at_day").over(w_all_by_claim_count))) / 60, 1).alias("minutes_since_prev_reported_at_day"),
        F.row_number().over(w_all_by_claim_count).alias("rn"),
        F.round(F.col("claim_count") / F.nullif(F.sum("claim_count").over(w_reported_at_day_month_claim), F.lit(0)), 4).alias("claim_count_share_of_reported_at_day_month_claim"),
        F.count(F.lit(1)).over(w_reported_at_day_month_claim).alias("rows_in_reported_at_day_month_claim_type_group"),
        F.nth_value("reported_at_day_day", 2).over(w_all_by_claim_count_full).alias("second_reported_at_day_day"),
        F.dense_rank().over(w_all_by_claim_count_ties).alias("claim_count_dense_rank"),
    )
)

claim_type_group_stats = (
    with_windows
    .groupBy("claim_type_group")
    .agg(F.avg("rn").alias("avg_rn"))
)

outliers = (
    with_windows.alias("s")
    .join(claim_type_group_stats.alias("g"), F.col("g.claim_type_group") == F.col("s.claim_type_group"), "inner")
    .filter(F.col("s.rn") > F.col("g.avg_rn"))
    .select(
        "s.*",
        "g.avg_rn",
        F.round(F.col("s.rn") - F.col("g.avg_rn"), 2).alias("rn_minus_avg"),
    )
)

result = (
    outliers
    .select(
        "id",
        "adjuster_id",
        "claim_count_share_of_adjuster_id",
        "reported_at_day_day",
        "claim_type_group_ref",
        "reported_at_day_month_policy_id_max_claim_count",
        "distinct_claim_type_in_reported_at_day_month_pol",
        "minutes_since_prev_reported_at_day",
        "rn",
        "second_reported_at_day_day",
        "claim_count_dense_rank",
        "avg_rn",
        (F.col("rn_minus_avg") + F.col("claim_count_dense_rank")).alias("rn_minus_avg_plus_claim_count_dense_rank"),
        F.coalesce(F.col("claim_type_group"), F.lit("UNKNOWN")).alias("claim_type_group_or_unknown"),
    )
    .orderBy(F.col("adjuster_id").asc())
)
