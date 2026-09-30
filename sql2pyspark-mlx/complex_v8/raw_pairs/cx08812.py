from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_status_policy_product = Window.partitionBy("policy_status", "policy_product")
w_policy_id = Window.partitionBy("policy_id")
w_policy_product_by_approved_a = Window.partitionBy("policy_product").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_product_by_approved_a_2 = Window.partitionBy("policy_product").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_product_by_approved_a_3 = Window.partitionBy("policy_product").orderBy(F.col("approved_amount").desc())
w_adjuster_id_by_approved_amou = Window.partitionBy("adjuster_id").orderBy("approved_amount", "id")
w_policy_product_adjuster_id_b = Window.partitionBy("policy_product", "adjuster_id").orderBy(F.col("claim_date").asc_nulls_last(), "id")
w_claim_date_rank_by_claim_dat = Window.partitionBy("claim_date_rank").orderBy("claim_date")
w_all_by_claim_date_running = Window.orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_date_rank_by_claim_dat_2 = Window.partitionBy("claim_date_rank").orderBy("claim_date", "id").rowsBetween(-2, Window.currentRow)
w_all_by_claim_date_moving_6 = Window.orderBy("claim_date", "id").rowsBetween(-6, Window.currentRow)

scoped_claims = (
    df_claims
    .filter((F.col("claimed_amount").between(4868.7, 44921.3)) & (F.col("claim_type").isin("delay", "fire", "theft")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        (F.col("approved_amount") - F.col("claimed_amount")).alias("approved_amount_minus_claimed_amount"),
    )
)

joined = (
    scoped_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(~F.col("p.status").isin("CANCELLED", "LAPSED"))
    .select(
        "s.*",
        F.col("p.status").alias("policy_status"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.product").alias("policy_product"),
    )
)

ranked = (
    joined
    .select(
        "*",
        F.max("approved_amount").over(w_policy_status_policy_product).alias("policy_status_policy_product_max_approved_amount"),
        F.size(F.collect_set("adjuster_id").over(w_policy_status_policy_product)).alias("distinct_adjuster_id_in_policy_status_policy_pro"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_policy_id), 2).alias("approved_amount_vs_policy_id_avg"),
        F.nth_value("claim_date", 2).over(w_policy_product_by_approved_a).alias("second_claim_date"),
        F.count(F.lit(1)).over(w_policy_product_by_approved_a_2).alias("running_count"),
        F.dense_rank().over(w_policy_product_by_approved_a_3).alias("approved_amount_dense_rank"),
        F.lag("policy_premium", 1, 0).over(w_adjuster_id_by_approved_amou).alias("prev_policy_premium"),
        F.rank().over(w_policy_product_adjuster_id_b).alias("claim_date_rank"),
        (F.col("policy_premium") - F.lag("policy_premium").over(w_policy_product_adjuster_id_b)).alias("policy_premium_change"),
    )
)

top_per_group = (
    ranked
    .filter(F.col("approved_amount_dense_rank") == 1)
    .drop("approved_amount_dense_rank")
)

windowed = (
    top_per_group
    .select(
        "id",
        "policy_id",
        "claim_date",
        "status",
        "approved_amount_minus_claimed_amount",
        "policy_premium",
        "policy_status_policy_product_max_approved_amount",
        "approved_amount_vs_policy_id_avg",
        "second_claim_date",
        "prev_policy_premium",
        "claim_date_rank",
        "policy_premium_change",
        F.dense_rank().over(w_claim_date_rank_by_claim_dat).alias("claim_date_dense_rank"),
        F.first("policy_premium", ignorenulls=True).over(w_all_by_claim_date_running).alias("first_known_policy_premium"),
        F.sum("policy_premium_change").over(w_policy_id).alias("policy_id_sum_policy_premium_change"),
        F.round(F.col("claim_date_rank") / F.nullif(F.sum("claim_date_rank").over(w_policy_id), F.lit(0)), 4).alias("claim_date_rank_share_of_policy_id"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_policy_id), 2).alias("claimed_amount_vs_policy_id_avg"),
        F.round(F.avg("prev_policy_premium").over(w_claim_date_rank_by_claim_dat_2), 2).alias("prev_policy_premium_avg_3"),
        F.sum("approved_amount").over(w_all_by_claim_date_moving_6).alias("approved_amount_sum_7"),
    )
)

result = (
    windowed
    .select(
        "id",
        "status",
        "approved_amount_minus_claimed_amount",
        "policy_premium",
        "approved_amount_vs_policy_id_avg",
        "second_claim_date",
        "prev_policy_premium",
        "claim_date_rank",
        "policy_premium_change",
        "policy_id_sum_policy_premium_change",
        "claim_date_rank_share_of_policy_id",
        "claimed_amount_vs_policy_id_avg",
        "prev_policy_premium_avg_3",
        "approved_amount_sum_7",
    )
    .orderBy("policy_id_sum_policy_premium_change")
)
