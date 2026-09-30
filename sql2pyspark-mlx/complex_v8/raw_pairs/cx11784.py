from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_claim_type_len = Window.partitionBy("policy_id", "claim_type_len")
w_all_by_claim_type_len_full = Window.orderBy("claim_type_len", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_date_month_by_approved = Window.partitionBy(F.date_trunc("month", "claim_date")).orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_claim_date = Window.orderBy("claim_date", "id")
w_claim_type_len_policy_id_by = Window.partitionBy("claim_type_len", "policy_id").orderBy(F.col("policy_end_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_claim_date_ties = Window.orderBy("claim_date")
w_adjuster_id_claim_type_len = Window.partitionBy("adjuster_id", "claim_type_len")
w_distinct_holder_region_in_po = Window.partitionBy("distinct_holder_region_in_policy_id_claim_type_l", F.year("reported_at_date")).orderBy(F.col("distinct_holder_region_in_policy_id_claim_type_l").desc(), "id").rowsBetween(-3, Window.currentRow)
w_reported_at_date_month = Window.partitionBy(F.date_trunc("month", "reported_at_date"))
w_distinct_holder_region_in_po_2 = Window.partitionBy("distinct_holder_region_in_policy_id_claim_type_l", F.year("reported_at_date")).orderBy(F.col("distinct_holder_region_in_policy_id_claim_type_l").desc(), "id")
w_last_claim_date_2_month_clai = Window.partitionBy(F.date_trunc("month", "last_claim_date_2"), "claim_type_len").orderBy(F.col("n_rows").desc_nulls_first())
w_all_by_last_claim_date_2 = Window.orderBy(F.col("last_claim_date_2").asc_nulls_last(), "claim_type_len", "claim_date_rank", "last_claim_date_ym")
w_last_claim_date_2_month_clai_2 = Window.partitionBy(F.date_trunc("month", "last_claim_date_2"), "claim_type_len").orderBy(F.col("n_rows").desc_nulls_first(), "claim_type_len", "claim_date_rank", "last_claim_date_ym")
w_all_by_last_claim_date_2_tie = Window.orderBy(F.col("last_claim_date_2").asc_nulls_last())

filtered = (
    df_claims
    .filter(F.col("policy_id") <= 12)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "reported_at",
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
    )
)

combined_details = (
    filtered.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.approved_amount",
        "s.reported_at_date",
        "s.claim_type_len",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("po.risk_score").alias("holder_risk_score"),
        F.col("po.region").alias("holder_region"),
    )
)

metrics = (
    combined_details
    .select(
        "*",
        F.size(F.collect_set("holder_region").over(w_policy_id_claim_type_len)).alias("distinct_holder_region_in_policy_id_claim_type_l"),
        F.last("claim_date").over(w_all_by_claim_type_len_full).alias("last_claim_date"),
        F.count(F.lit(1)).over(w_policy_id_claim_type_len).alias("rows_in_policy_id_claim_type_len"),
        F.min("approved_amount").over(w_claim_date_month_by_approved).alias("min_approved_amount_so_far"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_all_by_claim_date)).alias("days_since_prev_claim_date"),
        F.first("holder_risk_score", ignorenulls=True).over(w_claim_type_len_policy_id_by).alias("first_known_holder_risk_score"),
        F.rank().over(w_all_by_claim_date_ties).alias("claim_date_rank"),
        F.max("holder_risk_score").over(w_adjuster_id_claim_type_len).alias("adjuster_id_claim_type_len_max_holder_risk_score"),
        F.round(F.col("claim_type_len") / F.nullif(F.sum("claim_type_len").over(w_policy_id_claim_type_len), F.lit(0)), 4).alias("claim_type_len_share_of_policy_id_claim_type_len"),
    )
)

first_rows = metrics.filter((F.col("claim_date_rank") <= 3) & (F.col("holder_risk_score") < 0.6))

windowed = (
    first_rows
    .select(
        "*",
        F.round(F.avg("claim_date_rank").over(w_distinct_holder_region_in_po), 2).alias("claim_date_rank_avg_4"),
        F.round(F.col("days_since_prev_claim_date") / F.nullif(F.sum("days_since_prev_claim_date").over(w_reported_at_date_month), F.lit(0)), 4).alias("days_since_prev_claim_date_share_of_reported_at_"),
        F.round(F.avg("approved_amount").over(w_reported_at_date_month), 2).alias("reported_at_date_month_avg_approved_amount"),
        F.datediff(F.col("reported_at_date"), F.lag("reported_at_date").over(w_distinct_holder_region_in_po_2)).alias("days_since_prev_reported_at_date"),
    )
)

per_group = (
    windowed
    .groupBy(
        "claim_type_len",
        "claim_date_rank",
        F.date_format("last_claim_date", "yyyy-MM").alias("last_claim_date_ym"),
    )
    .agg(
        F.array_sort(F.collect_set("holder_region")).alias("holder_region_list"),
        F.max("claim_date").alias("last_claim_date_2"),
        F.round(F.avg("adjuster_id_claim_type_len_max_holder_risk_score"), 2).alias("avg_adjuster_id_claim_type_len_max_holder_risk_s"),
        F.sum("min_approved_amount_so_far").alias("total_min_approved_amount_so_far"),
        F.count("*").alias("n_rows"),
        F.avg("claim_date_rank").alias("avg_claim_date_rank_check"),
    )
    .filter(F.col("avg_claim_date_rank_check") > 2)
    .drop("avg_claim_date_rank_check")
)

with_windows = (
    per_group
    .select(
        "claim_type_len",
        "claim_date_rank",
        "last_claim_date_ym",
        "holder_region_list",
        "avg_adjuster_id_claim_type_len_max_holder_risk_s",
        "total_min_approved_amount_so_far",
        "n_rows",
        F.round(F.cume_dist().over(w_last_claim_date_2_month_clai), 3).alias("n_rows_cume_dist"),
        F.round(F.cume_dist().over(w_all_by_last_claim_date_2), 3).alias("last_claim_date_2_cume_dist"),
        F.lead("claim_type_len").over(w_last_claim_date_2_month_clai_2).alias("next_claim_type_len"),
        F.rank().over(w_all_by_last_claim_date_2_tie).alias("last_claim_date_2_rank"),
    )
)

result = (
    with_windows
    .select(
        "claim_type_len",
        "claim_date_rank",
        "last_claim_date_ym",
        "avg_adjuster_id_claim_type_len_max_holder_risk_s",
        "total_min_approved_amount_so_far",
        "n_rows",
        "n_rows_cume_dist",
        "last_claim_date_2_cume_dist",
        "next_claim_type_len",
        "last_claim_date_2_rank",
    )
    .orderBy(
        "last_claim_date_2_cume_dist",
        F.col("avg_adjuster_id_claim_type_len_max_holder_risk_s").desc(),
    )
)
