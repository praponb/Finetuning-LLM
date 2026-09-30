from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type = Window.partitionBy("claim_type")
w_adjuster_id_claim_type_by_re = Window.partitionBy("adjuster_id", "claim_type").orderBy(F.col("reported_at").desc(), "id")
w_all_by_claimed_amount = Window.orderBy("claimed_amount", "id")
w_all_by_claimed_amount_prior = Window.orderBy("claimed_amount", "id").rowsBetween(-2, -1)

recent_claims = (
    df_claims
    .filter((F.year(F.col("claim_date")) == 2024) & (F.col("status").isin("IN_REVIEW", "APPROVED", "OPEN")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.concat_ws(": ", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
        F.date_format(F.col("claim_date"), "MMM yyyy").alias("claim_date_label"),
    )
)

ranked = (
    recent_claims
    .select(
        "*",
        F.min("claimed_amount").over(w_claim_type).alias("claim_type_min_claimed_amount"),
        F.dense_rank().over(w_adjuster_id_claim_type_by_re).alias("reported_at_dense_rank"),
        F.round(F.cume_dist().over(w_all_by_claimed_amount), 3).alias("claimed_amount_cume_dist"),
        F.sum("claimed_amount").over(w_all_by_claimed_amount_prior).alias("claimed_amount_sum_2_prior"),
    )
)

with_parent_adjuster = (
    ranked.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
    )
)

result = (
    with_parent_adjuster
    .groupingSets(
        [["claim_type", "adjuster_id"], ["claim_type"], []],
        "claim_type", "adjuster_id",
    )
    .agg(
        F.round(F.stddev("reported_at_dense_rank"), 2).alias("stddev_reported_at_dense_rank"),
        F.count("*").alias("row_count"),
    )
    .select(
        F.coalesce(F.col("claim_type"), F.lit("ALL")).alias("claim_type"),
        "adjuster_id",
        "stddev_reported_at_dense_rank",
        "row_count",
    )
    .orderBy(
        F.col("claim_type").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
    )
)
