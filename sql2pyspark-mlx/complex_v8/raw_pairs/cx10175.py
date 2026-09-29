from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_holder_id_by_claimed = Window.partitionBy("policy_holder_id").orderBy("claimed_amount_bucket")
w_reported_at_to_date_policy_h = Window.partitionBy(F.to_date("reported_at"), "policy_holder_id")
w_adjuster_id_by_policy_premiu = Window.partitionBy("adjuster_id").orderBy(F.col("policy_premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_holder_id_by_claimed_2 = Window.partitionBy("policy_holder_id").orderBy("claimed_amount_bucket", "id")
w_adjuster_id_by_policy_premiu_2 = Window.partitionBy("adjuster_id").orderBy(F.col("policy_premium").desc(), "id")
w_all_by_approved_amount_runni = Window.orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_by_policy_premiu_3 = Window.partitionBy("adjuster_id").orderBy(F.col("policy_premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_next_policy_end_da = Window.partitionBy("policy_id", F.month("next_policy_end_date")).orderBy(F.col("reported_at_to_date_policy_holder_id_max_claimed").desc())
w_policy_premium_dense_rank_po = Window.partitionBy("policy_premium_dense_rank", F.month("policy_end_date"))
w_claimed_amount_bucket_dense = Window.partitionBy("claimed_amount_bucket_dense_rank_2", "adjuster_id").orderBy(F.col("policy_premium_dense_rank").desc(), "id").rowsBetween(-2, -1)

recent_claims = (
    df_claims
    .filter(F.col("claimed_amount") <= 31715.7)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
    )
)

with_details = (
    recent_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.reported_at",
        "s.claimed_amount_bucket",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

sequenced = (
    with_details
    .select(
        "*",
        F.dense_rank().over(w_policy_holder_id_by_claimed).alias("claimed_amount_bucket_dense_rank"),
        F.sum("approved_amount").over(w_reported_at_to_date_policy_h).alias("reported_at_to_date_policy_holder_id_sum_approve"),
        F.last("reported_at").over(w_adjuster_id_by_policy_premiu).alias("last_reported_at"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_reported_at_to_date_policy_h), 2).alias("approved_amount_vs_reported_at_to_date_policy_ho"),
        F.dense_rank().over(w_policy_holder_id_by_claimed_2).alias("claimed_amount_bucket_dense_rank_2"),
        F.dense_rank().over(w_adjuster_id_by_policy_premiu_2).alias("policy_premium_dense_rank"),
        F.max("claimed_amount_bucket").over(w_reported_at_to_date_policy_h).alias("reported_at_to_date_policy_holder_id_max_claimed"),
        F.min("approved_amount").over(w_all_by_approved_amount_runni).alias("min_approved_amount_so_far"),
        F.first("claimed_amount_bucket", ignorenulls=True).over(w_adjuster_id_by_policy_premiu_3).alias("first_known_claimed_amount_bucket"),
        F.round(F.col("claimed_amount_bucket") - F.avg("claimed_amount_bucket").over(w_reported_at_to_date_policy_h), 2).alias("claimed_amount_bucket_vs_reported_at_to_date_pol"),
        F.count(F.lit(1)).over(w_reported_at_to_date_policy_h).alias("rows_in_reported_at_to_date_policy_holder_id"),
        F.lead("policy_end_date").over(w_policy_holder_id_by_claimed_2).alias("next_policy_end_date"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_reported_at_to_date_policy_h), F.lit(0)), 4).alias("approved_amount_share_of_reported_at_to_date_pol"),
    )
)

top_rows = (
    sequenced
    .filter((F.col("policy_premium_dense_rank") <= 3) & (F.col("first_known_claimed_amount_bucket") < 42515))
)

with_windows = (
    top_rows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "reported_at",
        "policy_end_date",
        "claimed_amount_bucket_dense_rank",
        "last_reported_at",
        "policy_premium_dense_rank",
        "reported_at_to_date_policy_holder_id_max_claimed",
        "min_approved_amount_so_far",
        "first_known_claimed_amount_bucket",
        "claimed_amount_bucket_vs_reported_at_to_date_pol",
        "rows_in_reported_at_to_date_policy_holder_id",
        "next_policy_end_date",
        "approved_amount_share_of_reported_at_to_date_pol",
        F.round(F.cume_dist().over(w_policy_id_next_policy_end_da), 3).alias("reported_at_to_date_policy_holder_id_max_claimed_2"),
        F.sum("approved_amount").over(w_policy_premium_dense_rank_po).alias("policy_premium_dense_rank_policy_end_date_month_"),
        F.round(F.avg("approved_amount").over(w_claimed_amount_bucket_dense), 2).alias("approved_amount_avg_2_prior"),
    )
)

result = (
    with_windows
    .filter(F.col("reported_at_to_date_policy_holder_id_max_claimed_2") >= 0.1)
    .select(
        "id",
        "policy_id",
        "claimed_amount_bucket_dense_rank",
        "policy_premium_dense_rank",
        "first_known_claimed_amount_bucket",
        "claimed_amount_bucket_vs_reported_at_to_date_pol",
        "next_policy_end_date",
        "approved_amount_share_of_reported_at_to_date_pol",
        "reported_at_to_date_policy_holder_id_max_claimed_2",
        "policy_premium_dense_rank_policy_end_date_month_",
        F.date_format(F.col("last_reported_at"), "MMM yyyy").alias("last_reported_at_label"),
    )
    .orderBy(F.col("reported_at_to_date_policy_holder_id_max_claimed_2").desc())
)
