from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_quarter_by_claim = Window.partitionBy("claim_date_quarter").orderBy("claim_date_plus_1m", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_product_by_claim_d = Window.partitionBy("policy_id", "product").orderBy("claim_date_plus_1m", "id")
w_claim_date_quarter_by_claim_2 = Window.partitionBy("claim_date_quarter").orderBy("claim_date_plus_1m", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_date_quarter = Window.partitionBy("claim_date_quarter")
w_product_by_last_claimed_amou = Window.partitionBy("product").orderBy("last_claimed_amount", "id").rowsBetween(-3, -1)
w_product = Window.partitionBy("product")
w_all_by_premium_prior_6 = Window.orderBy(F.col("premium").desc(), "id").rowsBetween(-6, -1)
w_claim_date_quarter_by_claim_3 = Window.partitionBy("claim_date_quarter").orderBy("claim_date_plus_1m_percent_rank")
w_all_by_premium = Window.orderBy(F.col("premium").desc(), "id")

joined_ranges = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "inner",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.adjuster_id",
        "c.status",
        "p.product",
        "p.premium",
    )
)

annotated = (
    joined_ranges
    .filter(F.col("claim_date") >= F.lit("2024-03-24").cast("date"))
    .select(
        "id",
        "policy_id",
        "claimed_amount",
        "product",
        "premium",
        F.quarter(F.col("claim_date")).alias("claim_date_quarter"),
        F.round(F.col("premium") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("premium_per_claimed_amount"),
        F.add_months(F.col("claim_date"), 1).alias("claim_date_plus_1m"),
    )
)

ranked = (
    annotated
    .select(
        "*",
        F.last("claimed_amount").over(w_claim_date_quarter_by_claim).alias("last_claimed_amount"),
        F.lag("premium").over(w_policy_id_product_by_claim_d).alias("prev_premium"),
        F.sum("claimed_amount").over(w_claim_date_quarter_by_claim_2).alias("running_claimed_amount"),
        F.round(F.col("premium_per_claimed_amount") / F.nullif(F.sum("premium_per_claimed_amount").over(w_claim_date_quarter), F.lit(0)), 4).alias("premium_per_claimed_amount_share_of_claim_date_q"),
        F.round(F.percent_rank().over(w_policy_id_product_by_claim_d), 3).alias("claim_date_plus_1m_percent_rank"),
    )
)

metrics = (
    ranked
    .select(
        "id",
        "policy_id",
        "premium",
        "claim_date_quarter",
        "claim_date_plus_1m",
        "prev_premium",
        "premium_per_claimed_amount_share_of_claim_date_q",
        "claim_date_plus_1m_percent_rank",
        F.sum("claim_date_quarter").over(w_product_by_last_claimed_amou).alias("claim_date_quarter_sum_3_prior"),
        F.size(F.collect_set("policy_id").over(w_product)).alias("distinct_policy_id_in_product"),
        F.round(F.avg("last_claimed_amount").over(w_all_by_premium_prior_6), 2).alias("last_claimed_amount_avg_6_prior"),
        F.round(F.cume_dist().over(w_claim_date_quarter_by_claim_3), 3).alias("claim_date_plus_1m_percent_rank_cume_dist"),
        (F.col("prev_premium") - F.lag("prev_premium").over(w_all_by_premium)).alias("prev_premium_change"),
        F.round(F.col("prev_premium") / F.nullif(F.sum("prev_premium").over(w_product), F.lit(0)), 4).alias("prev_premium_share_of_product"),
    )
)

result = (
    metrics
    .filter(F.col("premium_per_claimed_amount_share_of_claim_date_q").between(0.1, 0.7))
    .select(
        "id",
        "policy_id",
        "premium",
        "claim_date_plus_1m",
        "prev_premium",
        "premium_per_claimed_amount_share_of_claim_date_q",
        "claim_date_plus_1m_percent_rank",
        "claim_date_quarter_sum_3_prior",
        "distinct_policy_id_in_product",
        "prev_premium_change",
        "prev_premium_share_of_product",
        F.greatest(F.col("prev_premium_change"), F.col("claim_date_quarter")).alias("greatest_prev_premium_change_claim_date_quarter"),
    )
    .orderBy(
        F.col("distinct_policy_id_in_product").desc(),
        F.col("greatest_prev_premium_change_claim_date_quarter").asc_nulls_last(),
    )
)
