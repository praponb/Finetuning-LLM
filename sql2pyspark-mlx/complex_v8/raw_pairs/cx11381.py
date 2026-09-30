from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_claim_status_not_in_open_i = Window.partitionBy("n_claim_status_not_in_open_in", "claim_date_year").orderBy("distinct_ids", "claim_date_year", "claim_id", "adjuster_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_claim_status_not_in_open_i_2 = Window.partitionBy("n_claim_status_not_in_open_in", "claim_date_year").orderBy("distinct_ids")
w_claim_date_year_by_n_claim_s = Window.partitionBy("claim_date_year").orderBy(F.col("n_claim_status_not_in_open_in").desc())
w_n_claim_status_not_in_open_i_3 = Window.partitionBy("n_claim_status_not_in_open_in", "claim_date_year").orderBy("distinct_ids", "claim_date_year", "claim_id", "adjuster_id").rowsBetween(-2, Window.currentRow)
w_n_claim_status_not_in_open_i_4 = Window.partitionBy("n_claim_status_not_in_open_in", "claim_date_year").orderBy("distinct_ids", "claim_date_year", "claim_id", "adjuster_id").rowsBetween(-3, Window.currentRow)

filtered_claims = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "status",
        "reported_at",
        F.date_format(F.col("claim_date"), "yyyy-MM-dd").alias("claim_date_label"),
        F.greatest(F.col("claimed_amount"), F.col("approved_amount")).alias("greatest_claimed_amount_approved_amount"),
    )
)

annotated = filtered_claims.withColumn("reported_at_date", F.to_date(F.col("reported_at")))

qualified = (
    annotated.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left_semi")
    .filter(F.col("adjuster_id") <= 4)
)

with_children_claims = (
    qualified.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.adjuster_id") >= 3), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.reported_at",
        "s.claim_date_label",
        "s.greatest_claimed_amount_approved_amount",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        "c.claim_date",
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

per_group = (
    with_children_claims
    .filter(F.col("adjuster_id") <= 7)
    .groupBy(F.year("claim_date").alias("claim_date_year"), "claim_id", "adjuster_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("claim_approved_amount").alias("total_claim_approved_amount"),
        F.min("greatest_claimed_amount_approved_amount").alias("min_greatest_claimed_amount_approved_amount"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(~F.col("claim_status").isin("OPEN", "IN_REVIEW"), 1)).alias("n_claim_status_not_in_open_in"),
    )
)

filtered = (
    per_group
    .crossJoin(per_group.agg(F.min("min_greatest_claimed_amount_approved_amount").alias("overall_min_min_greatest_claimed_amount_approved")))
    .filter(F.col("min_greatest_claimed_amount_approved_amount") > F.col("overall_min_min_greatest_claimed_amount_approved"))
    .drop("overall_min_min_greatest_claimed_amount_approved")
)

with_trends = (
    filtered
    .select(
        "claim_date_year",
        "claim_id",
        "adjuster_id",
        "n_rows",
        "total_claim_approved_amount",
        "min_greatest_claimed_amount_approved_amount",
        "n_claim_status_not_in_open_in",
        F.sum("n_rows").over(w_n_claim_status_not_in_open_i).alias("running_n_rows"),
        F.rank().over(w_n_claim_status_not_in_open_i_2).alias("distinct_ids_rank"),
        F.round(F.cume_dist().over(w_claim_date_year_by_n_claim_s), 3).alias("n_claim_status_not_in_open_in_cume_dist"),
        F.sum("total_claim_approved_amount").over(w_n_claim_status_not_in_open_i).alias("running_total_claim_approved_amount"),
        F.max("n_rows").over(w_n_claim_status_not_in_open_i_3).alias("n_rows_max_3"),
        F.sum("claim_date_year").over(w_n_claim_status_not_in_open_i_4).alias("claim_date_year_sum_4"),
    )
)

result = (
    with_trends
    .filter(F.col("claim_id") <= 90)
    .select(
        "claim_date_year",
        "claim_id",
        "adjuster_id",
        "total_claim_approved_amount",
        "min_greatest_claimed_amount_approved_amount",
        "n_claim_status_not_in_open_in",
        "running_n_rows",
        "distinct_ids_rank",
        "n_rows_max_3",
        "claim_date_year_sum_4",
    )
)
