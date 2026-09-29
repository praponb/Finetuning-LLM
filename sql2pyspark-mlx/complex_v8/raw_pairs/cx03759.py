from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claimed_amount_ties = Window.orderBy(F.col("claimed_amount").desc_nulls_first())
w_adjuster_id_by_claimed_amoun = Window.partitionBy("adjuster_id").orderBy(F.col("claimed_amount").desc_nulls_first())
w_all_by_reported_at_full = Window.orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id = Window.partitionBy("adjuster_id")
w_all_by_claimed_amount = Window.orderBy(F.col("claimed_amount").desc_nulls_first(), "id")
w_reported_at_year = Window.partitionBy(F.year("reported_at"))
w_all_by_claimed_amount_prior = Window.orderBy(F.col("claimed_amount").desc_nulls_first(), "id").rowsBetween(-3, -1)
w_all_by_claimed_amount_full = Window.orderBy(F.col("claimed_amount").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_reported_at_ties = Window.orderBy(F.col("reported_at").desc())
w_adjuster_id_by_reported_at_f = Window.partitionBy("adjuster_id").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_adjuster_ids = Window.partitionBy("distinct_adjuster_ids")
w_distinct_adjuster_ids_by_avg = Window.partitionBy("distinct_adjuster_ids").orderBy("avg_claimed_amount", "adjuster_id", "reported_at_rank", "claim_type")

cleaned_claims = (
    df_claims
    .filter(F.to_date(F.col("reported_at")) < F.lit("2025-02-12").cast("date"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "reported_at",
    )
)

sequenced = (
    cleaned_claims
    .select(
        "*",
        F.dense_rank().over(w_all_by_claimed_amount_ties).alias("claimed_amount_dense_rank"),
        F.dense_rank().over(w_adjuster_id_by_claimed_amoun).alias("claimed_amount_dense_rank_2"),
        F.nth_value("reported_at", 2).over(w_all_by_reported_at_full).alias("second_reported_at"),
        F.min("claimed_amount").over(w_adjuster_id).alias("adjuster_id_min_claimed_amount"),
        F.ntile(5).over(w_all_by_claimed_amount).alias("claimed_amount_quintile"),
        F.max("claimed_amount").over(w_reported_at_year).alias("reported_at_year_max_claimed_amount"),
        F.size(F.collect_set("claim_type").over(w_reported_at_year)).alias("distinct_claim_type_in_reported_at_year"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_reported_at_year), F.lit(0)), 4).alias("claimed_amount_share_of_reported_at_year"),
        F.sum("claimed_amount").over(w_adjuster_id).alias("adjuster_id_sum_claimed_amount"),
        F.sum("claimed_amount").over(w_all_by_claimed_amount_prior).alias("claimed_amount_sum_3_prior"),
        F.nth_value("reported_at", 2).over(w_all_by_claimed_amount_full).alias("second_reported_at_2"),
        F.rank().over(w_all_by_reported_at_ties).alias("reported_at_rank"),
        F.min("claimed_amount").over(w_reported_at_year).alias("reported_at_year_min_claimed_amount"),
        F.nth_value("claimed_amount", 2).over(w_adjuster_id_by_reported_at_f).alias("second_claimed_amount"),
    )
)

top_rows = (
    sequenced
    .filter((F.col("claimed_amount_dense_rank_2") == 1) & (F.col("reported_at_year_max_claimed_amount") >= 50090.0))
    .drop("claimed_amount_dense_rank_2")
)

totals = (
    top_rows
    .groupBy("adjuster_id", "reported_at_rank", "claim_type")
    .agg(
        F.round(F.avg("claimed_amount"), 2).alias("avg_claimed_amount"),
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
        F.min("second_claimed_amount").alias("min_second_claimed_amount"),
        F.round(100 * F.count(F.when(~F.col("claim_type").isin("collision", "fire"), 1)) / F.count("*"), 1).alias("pct_claim_type_not_in_collision"),
        F.round(F.sum("adjuster_id_min_claimed_amount"), 2).alias("total_adjuster_id_min_claimed_amount"),
        F.round(F.avg("claimed_amount_share_of_reported_at_year"), 2).alias("avg_claimed_amount_share_of_reported_at_year"),
    )
)

with_trends = (
    totals
    .select(
        "*",
        F.round(F.col("avg_claimed_amount") - F.avg("avg_claimed_amount").over(w_distinct_adjuster_ids), 2).alias("avg_claimed_amount_vs_distinct_adjuster_ids_avg"),
        F.lag("reported_at_rank", 1, 0).over(w_distinct_adjuster_ids_by_avg).alias("prev_reported_at_rank"),
    )
)

result = (
    with_trends
    .filter(F.col("avg_claimed_amount_share_of_reported_at_year") > 0.2)
    .select(
        "adjuster_id",
        "avg_claimed_amount",
        "distinct_adjuster_ids",
        "min_second_claimed_amount",
        "pct_claim_type_not_in_collision",
        "total_adjuster_id_min_claimed_amount",
        "avg_claimed_amount_share_of_reported_at_year",
        "avg_claimed_amount_vs_distinct_adjuster_ids_avg",
    )
    .orderBy(F.col("avg_claimed_amount").asc())
)
