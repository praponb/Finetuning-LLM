from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_dayofweek = Window.partitionBy("claim_date_dayofweek")
w_policy_id_by_claim_date_plus = Window.partitionBy("policy_id").orderBy("claim_date_plus_90d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_claimed_amount = Window.orderBy(F.col("claimed_amount").desc_nulls_first(), "id")
w_all_by_claimed_amount_ties = Window.orderBy(F.col("claimed_amount").desc_nulls_first())
w_all_by_claimed_amount_runnin = Window.orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_by_claim_date_plus_2 = Window.partitionBy("policy_id").orderBy("claim_date_plus_90d", "id").rowsBetween(-6, Window.currentRow)
w_all_by_claimed_amount_prior = Window.orderBy("claimed_amount", "id").rowsBetween(-2, -1)
w_distinct_policy_id_in_claim = Window.partitionBy("distinct_policy_id_in_claim_date_dayofweek", "adjuster_id").orderBy("distinct_policy_id_in_claim_date_dayofweek")
w_adjuster_id = Window.partitionBy("adjuster_id")
w_claimed_amount_decile_bucket = Window.partitionBy("claimed_amount_decile_bucket").orderBy(F.col("distinct_policy_id_in_claim_date_dayofweek").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_claims
    .filter((F.col("claimed_amount") >= 18778.6) | (F.col("adjuster_id") <= 5))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.date_add(F.col("claim_date"), 90).alias("claim_date_plus_90d"),
        F.dayofweek(F.col("claim_date")).alias("claim_date_dayofweek"),
    )
)

metrics = (
    prepared
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "reported_at",
        F.round(F.avg("claim_date_dayofweek").over(w_claim_date_dayofweek), 2).alias("claim_date_dayofweek_avg_claim_date_dayofweek"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_claim_date_dayofweek), 2).alias("approved_amount_vs_claim_date_dayofweek_avg"),
        F.count(F.lit(1)).over(w_claim_date_dayofweek).alias("rows_in_claim_date_dayofweek"),
        F.last("reported_at", ignorenulls=True).over(w_policy_id_by_claim_date_plus).alias("last_known_reported_at"),
        F.ntile(10).over(w_all_by_claimed_amount).alias("claimed_amount_decile"),
        F.dense_rank().over(w_all_by_claimed_amount_ties).alias("claimed_amount_dense_rank"),
        F.count(F.lit(1)).over(w_all_by_claimed_amount_runnin).alias("running_count"),
        F.size(F.collect_set("policy_id").over(w_claim_date_dayofweek)).alias("distinct_policy_id_in_claim_date_dayofweek"),
        F.round(F.avg("claim_date_dayofweek").over(w_policy_id_by_claim_date_plus_2), 2).alias("claim_date_dayofweek_avg_7"),
        F.sum("claim_date_dayofweek").over(w_claim_date_dayofweek).alias("claim_date_dayofweek_sum_claim_date_dayofweek"),
        F.round(F.avg("approved_amount").over(w_all_by_claimed_amount_prior), 2).alias("approved_amount_avg_2_prior"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claim_date_dayofweek_avg_claim_date_dayofweek",
        "running_count",
        "distinct_policy_id_in_claim_date_dayofweek",
        "approved_amount_avg_2_prior",
        F.to_date(F.col("last_known_reported_at")).alias("last_known_reported_at_date"),
        (F.floor(F.col("claimed_amount_decile") / 2) * 2).alias("claimed_amount_decile_bucket"),
        F.year(F.col("claim_date")).alias("claim_date_year"),
    )
)

ranked = (
    enriched
    .select(
        "*",
        F.round(F.percent_rank().over(w_distinct_policy_id_in_claim), 3).alias("distinct_policy_id_in_claim_date_dayofweek_perce"),
        F.round(F.col("claim_date_year") - F.avg("claim_date_year").over(w_adjuster_id), 2).alias("claim_date_year_vs_adjuster_id_avg"),
        F.last("claim_date", ignorenulls=True).over(w_claimed_amount_decile_bucket).alias("last_known_claim_date"),
        F.first("last_known_reported_at_date", ignorenulls=True).over(w_claimed_amount_decile_bucket).alias("first_known_last_known_reported_at_date"),
        F.round(F.col("claim_date_year") / F.nullif(F.sum("claim_date_year").over(w_adjuster_id), F.lit(0)), 4).alias("claim_date_year_share_of_adjuster_id"),
    )
)

result = (
    ranked
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claim_date_dayofweek_avg_claim_date_dayofweek",
        "running_count",
        "distinct_policy_id_in_claim_date_dayofweek",
        "approved_amount_avg_2_prior",
        "last_known_reported_at_date",
        "claim_date_year",
        "distinct_policy_id_in_claim_date_dayofweek_perce",
        "claim_date_year_vs_adjuster_id_avg",
        "last_known_claim_date",
        "first_known_last_known_reported_at_date",
    )
    .orderBy(
        "claim_date_dayofweek_avg_claim_date_dayofweek",
        F.col("approved_amount_avg_2_prior").desc(),
    )
)
