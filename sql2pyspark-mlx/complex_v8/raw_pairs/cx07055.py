from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("policy_id").orderBy(F.col("reported_at").desc(), F.col("id").desc())
w_dedup = Window.partitionBy("claim_type", "adjuster_id").orderBy(F.col("claim_date").desc(), F.col("id").desc())
w_distinct_claim_type_groups_b = Window.partitionBy("distinct_claim_type_groups").orderBy(F.col("n_rows").desc(), "reported_at_month", "reported_at_plus_2h_month")
w_distinct_claim_type_groups_b_2 = Window.partitionBy("distinct_claim_type_groups").orderBy(F.col("n_rows").desc(), "reported_at_month", "reported_at_plus_2h_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.when(F.col("claim_type").isin("theft", "flood"), "group_a").when(F.col("claim_type") == "medical", "group_b").otherwise("other").alias("claim_type_group"),
        F.date_format(F.col("reported_at"), "yyyy-MM-dd HH:00").alias("reported_at_label"),
    )
)

unique_rows = (
    valid
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

unique_rows_2 = (
    unique_rows
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

annotated = (
    unique_rows_2
    .filter(F.col("reported_at") >= F.lit("2025-01-19 12:00:00").cast("timestamp"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        "claim_type_group",
        "reported_at_label",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
        (F.col("reported_at") + F.expr("INTERVAL 2 HOURS")).alias("reported_at_plus_2h"),
    )
)

stats = (
    annotated
    .groupBy(
        F.date_trunc("month", "reported_at").alias("reported_at_month"),
        F.date_trunc("month", "reported_at_plus_2h").alias("reported_at_plus_2h_month"),
    )
    .agg(
        F.round(F.avg("days_since_claim_date"), 2).alias("avg_days_since_claim_date"),
        F.countDistinct("claim_type_group").alias("distinct_claim_type_groups"),
        F.count("*").alias("n_rows"),
        F.percentile_approx("claimed_amount", 0.5).alias("median_claimed_amount"),
    )
)

without_matches = (
    stats
    .crossJoin(stats.agg(F.max("n_rows").alias("overall_max_n_rows")))
    .filter(F.col("n_rows") < F.col("overall_max_n_rows"))
    .drop("overall_max_n_rows")
)

ranked = (
    without_matches
    .select(
        "reported_at_month",
        "reported_at_plus_2h_month",
        "avg_days_since_claim_date",
        "distinct_claim_type_groups",
        "n_rows",
        "median_claimed_amount",
        F.lag("avg_days_since_claim_date", 3).over(w_distinct_claim_type_groups_b).alias("prev3_avg_days_since_claim_date"),
        F.nth_value("distinct_claim_type_groups", 2).over(w_distinct_claim_type_groups_b_2).alias("second_distinct_claim_type_groups"),
        F.first("median_claimed_amount").over(w_distinct_claim_type_groups_b_2).alias("first_median_claimed_amount"),
    )
)

result = (
    ranked
    .filter(F.col("median_claimed_amount") >= 12575.0)
    .select(
        "reported_at_month",
        "reported_at_plus_2h_month",
        "avg_days_since_claim_date",
        "n_rows",
        "median_claimed_amount",
        "prev3_avg_days_since_claim_date",
        "second_distinct_claim_type_groups",
        "first_median_claimed_amount",
        F.round(F.col("median_claimed_amount") * 1.07, 2).alias("median_claimed_amount_adj"),
        (F.floor(F.col("n_rows") / 2) * 2).alias("n_rows_bucket"),
    )
    .orderBy(F.col("avg_days_since_claim_date").desc_nulls_first())
)
