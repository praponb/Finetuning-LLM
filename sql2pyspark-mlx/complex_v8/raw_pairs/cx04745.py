from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("adjuster_id").orderBy(F.col("approved_amount_adj").desc(), F.col("id").desc())
w_all_by_days_since_reported_a = Window.orderBy(F.col("days_since_reported_at").desc(), "id")
w_reported_at_month = Window.partitionBy(F.month("reported_at"))
w_days_since_reported_at_quart = Window.partitionBy("days_since_reported_at_quartile").orderBy(F.col("rows_in_reported_at_month").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_days_since_reported_at_quart_2 = Window.partitionBy("days_since_reported_at_quartile")
w_days_since_reported_at_quart_3 = Window.partitionBy("days_since_reported_at_quartile").orderBy(F.col("days_since_reported_at").asc_nulls_last(), "id")

prepared_claims = (
    df_claims
    .filter(~F.col("status").isin("REJECTED", "IN_REVIEW"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.least(F.col("claimed_amount"), F.col("approved_amount")).alias("least_claimed_amount_approved_amount"),
        F.least(F.col("approved_amount"), F.col("claimed_amount")).alias("least_approved_amount_claimed_amount"),
        F.round(F.col("approved_amount") * 0.9, 2).alias("approved_amount_adj"),
    )
)

without_matches = (
    prepared_claims
    .crossJoin(prepared_claims.agg(F.max("claimed_amount").alias("overall_max_claimed_amount")))
    .filter(F.col("claimed_amount") < F.col("overall_max_claimed_amount"))
    .drop("overall_max_claimed_amount")
)

latest_per_key = (
    without_matches
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

derived = (
    latest_per_key
    .select(
        "id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "least_claimed_amount_approved_amount",
        "least_approved_amount_claimed_amount",
        "approved_amount_adj",
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
    )
)

sequenced = (
    derived
    .select(
        "id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "least_claimed_amount_approved_amount",
        "least_approved_amount_claimed_amount",
        "days_since_reported_at",
        F.ntile(4).over(w_all_by_days_since_reported_a).alias("days_since_reported_at_quartile"),
        F.count(F.lit(1)).over(w_reported_at_month).alias("rows_in_reported_at_month"),
    )
)

screened = (
    sequenced
    .crossJoin(sequenced.agg(F.avg("approved_amount").alias("overall_avg_approved_amount")))
    .filter(F.col("approved_amount") > F.col("overall_avg_approved_amount"))
    .drop("overall_avg_approved_amount")
)

sequenced_2 = (
    screened
    .select(
        "id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "least_claimed_amount_approved_amount",
        "days_since_reported_at",
        "days_since_reported_at_quartile",
        "rows_in_reported_at_month",
        F.first("rows_in_reported_at_month", ignorenulls=True).over(w_days_since_reported_at_quart).alias("first_known_rows_in_reported_at_month"),
        F.min("rows_in_reported_at_month").over(w_days_since_reported_at_quart_2).alias("days_since_reported_at_quartile_min_rows_in_repo"),
        F.sum("claimed_amount").over(w_days_since_reported_at_quart_2).alias("days_since_reported_at_quartile_sum_claimed_amou"),
        F.lead("approved_amount", 1, 0).over(w_days_since_reported_at_quart_3).alias("next_approved_amount"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_days_since_reported_at_quart_2), 2).alias("claimed_amount_vs_days_since_reported_at_quartil"),
    )
)

result = (
    sequenced_2
    .select(
        "id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "days_since_reported_at",
        "days_since_reported_at_quartile",
        "rows_in_reported_at_month",
        "first_known_rows_in_reported_at_month",
        "days_since_reported_at_quartile_min_rows_in_repo",
        "next_approved_amount",
    )
    .orderBy(F.col("reported_at").desc())
)
