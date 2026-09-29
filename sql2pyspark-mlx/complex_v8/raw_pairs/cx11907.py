from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reported_at_year_status = Window.partitionBy("reported_at_year", "status")
w_n_rows_reported_at_year_by_m = Window.partitionBy("n_rows", "reported_at_year").orderBy("median_claimed_amount")
w_claim_date_month_year_status = Window.partitionBy(F.year("claim_date_month"), "status").orderBy(F.col("avg_claimed_amount").asc_nulls_last(), "claim_date_month", "reported_at_year", "status").rowsBetween(-2, Window.currentRow)
w_claim_date_month_year_status_2 = Window.partitionBy(F.year("claim_date_month"), "status").orderBy(F.col("avg_claimed_amount").asc_nulls_last(), "claim_date_month", "reported_at_year", "status")

prepared_claims = df_claims.filter(F.col("claimed_amount") <= 33690.0)

rollup_base = (
    prepared_claims
    .groupBy(
        F.date_trunc("month", "claim_date").alias("claim_date_month"),
        F.year("reported_at").alias("reported_at_year"),
        "status",
    )
    .agg(
        F.round(F.avg("claimed_amount"), 2).alias("avg_claimed_amount"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.min("approved_amount").alias("min_approved_amount"),
        F.percentile_approx("claimed_amount", 0.5).alias("median_claimed_amount"),
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
    )
)

ranked = (
    rollup_base
    .select(
        "*",
        F.min("median_claimed_amount").over(w_reported_at_year_status).alias("reported_at_year_status_min_median_claimed_amoun"),
        F.count(F.lit(1)).over(w_reported_at_year_status).alias("rows_in_reported_at_year_status"),
        F.round(F.percent_rank().over(w_n_rows_reported_at_year_by_m), 3).alias("median_claimed_amount_percent_rank"),
        F.round(F.avg("median_claimed_amount").over(w_claim_date_month_year_status), 2).alias("median_claimed_amount_avg_3"),
        F.dense_rank().over(w_claim_date_month_year_status_2).alias("avg_claimed_amount_dense_rank"),
        F.round(F.col("median_claimed_amount") - F.avg("median_claimed_amount").over(w_reported_at_year_status), 2).alias("median_claimed_amount_vs_reported_at_year_status"),
    )
)

result = (
    ranked
    .select(
        "claim_date_month",
        "status",
        "status_list",
        "min_approved_amount",
        "median_claimed_amount",
        "n_rows",
        "claim_type_list",
        "reported_at_year_status_min_median_claimed_amoun",
        "median_claimed_amount_avg_3",
        "avg_claimed_amount_dense_rank",
        "median_claimed_amount_vs_reported_at_year_status",
        F.coalesce(F.col("min_approved_amount"), F.lit(0.0)).alias("min_approved_amount_filled"),
    )
    .orderBy(F.col("median_claimed_amount").desc())
)
