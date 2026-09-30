from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_adjuster_ids_claim = Window.partitionBy("distinct_adjuster_ids", "claim_type").orderBy(F.col("total_approved_amount").desc(), "claim_type", "reported_at_year").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reported_at_year_n_rows_by_n = Window.partitionBy("reported_at_year", "n_rows").orderBy(F.col("n_rows").desc(), "claim_type", "reported_at_year").rowsBetween(-6, -1)
w_distinct_adjuster_ids = Window.partitionBy("distinct_adjuster_ids")

scoped_claims = df_claims.filter(F.col("claimed_amount") >= 5313.8)

rollup_base = (
    scoped_claims
    .groupBy("claim_type", F.year("reported_at").alias("reported_at_year"))
    .agg(
        F.sum("approved_amount").alias("total_approved_amount"),
        F.count("*").alias("n_rows"),
        F.percentile_approx("claimed_amount", 0.5).alias("median_claimed_amount"),
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
    )
    .filter(F.col("n_rows") >= 3)
)

qualified = (
    rollup_base
    .crossJoin(rollup_base.agg(F.max("n_rows").alias("overall_max_n_rows")))
    .filter(F.col("n_rows") < F.col("overall_max_n_rows"))
    .drop("overall_max_n_rows")
)

with_windows = (
    qualified
    .select(
        "*",
        F.nth_value("n_rows", 2).over(w_distinct_adjuster_ids_claim).alias("second_n_rows"),
        F.first("total_approved_amount").over(w_distinct_adjuster_ids_claim).alias("first_total_approved_amount"),
        F.sum("distinct_adjuster_ids").over(w_reported_at_year_n_rows_by_n).alias("distinct_adjuster_ids_sum_6_prior"),
        F.round(F.col("median_claimed_amount") - F.avg("median_claimed_amount").over(w_distinct_adjuster_ids), 2).alias("median_claimed_amount_vs_distinct_adjuster_ids_a"),
    )
)

result = (
    with_windows
    .groupBy("reported_at_year")
    .pivot("claim_type", ["theft", "collision", "flood"])
    .agg(F.sum("median_claimed_amount_vs_distinct_adjuster_ids_a").alias("amount"), F.count("median_claimed_amount_vs_distinct_adjuster_ids_a").alias("n"))
)
