from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_rows_last_reported_at_year = Window.partitionBy("n_rows", F.year("last_reported_at")).orderBy("stddev_claimed_amount", "adjuster_id", "policy_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_rows_last_reported_at_year_2 = Window.partitionBy("n_rows", F.year("last_reported_at")).orderBy("stddev_claimed_amount")
w_n_rows = Window.partitionBy("n_rows")
w_distinct_ids_by_distinct_ids = Window.partitionBy("distinct_ids").orderBy(F.col("distinct_ids").desc(), "adjuster_id", "policy_id")
w_n_rows_last_reported_at_year_3 = Window.partitionBy("n_rows", F.year("last_reported_at")).orderBy("stddev_claimed_amount", "adjuster_id", "policy_id").rowsBetween(-3, -1)

cleaned = (
    df_claims
    .filter((F.col("policy_id") <= 9) & (F.col("status") == "PAID"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.concat(F.col("claim_type"), F.lit("#"), F.col("id").cast("string")).alias("claim_type_ref"),
        F.date_format(F.col("claim_date"), "yyyy-MM-dd").alias("claim_date_label"),
    )
)

stats = (
    cleaned
    .filter(F.col("claim_type_ref").isNotNull())
    .groupBy("adjuster_id", "policy_id")
    .agg(
        F.sum("claimed_amount").alias("total_claimed_amount"),
        F.min("reported_at").alias("first_reported_at"),
        F.max("reported_at").alias("last_reported_at"),
        F.count("*").alias("n_rows"),
        F.round(F.stddev("claimed_amount"), 2).alias("stddev_claimed_amount"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

screened = (
    stats
    .crossJoin(stats.agg(F.avg("total_claimed_amount").alias("overall_avg_total_claimed_amount")))
    .filter(F.col("total_claimed_amount") > F.col("overall_avg_total_claimed_amount"))
    .drop("overall_avg_total_claimed_amount")
)

with_activity = (
    screened.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left_semi")
)

with_windows = (
    with_activity
    .select(
        "*",
        F.nth_value("first_reported_at", 2).over(w_n_rows_last_reported_at_year).alias("second_first_reported_at"),
        F.round(F.cume_dist().over(w_n_rows_last_reported_at_year_2), 3).alias("stddev_claimed_amount_cume_dist"),
        F.round(F.col("stddev_claimed_amount") / F.nullif(F.sum("stddev_claimed_amount").over(w_n_rows), F.lit(0)), 4).alias("stddev_claimed_amount_share_of_n_rows"),
        F.rank().over(w_distinct_ids_by_distinct_ids).alias("distinct_ids_rank"),
        F.round(F.col("distinct_ids") / F.nullif(F.sum("distinct_ids").over(w_n_rows), F.lit(0)), 4).alias("distinct_ids_share_of_n_rows"),
        F.max("stddev_claimed_amount").over(w_n_rows_last_reported_at_year_3).alias("stddev_claimed_amount_max_3_prior"),
    )
)

result = (
    with_windows
    .select(
        "adjuster_id",
        "policy_id",
        "total_claimed_amount",
        "first_reported_at",
        "last_reported_at",
        "stddev_claimed_amount",
        "distinct_ids",
        "second_first_reported_at",
        "stddev_claimed_amount_share_of_n_rows",
        "distinct_ids_rank",
        "distinct_ids_share_of_n_rows",
        F.round(F.col("n_rows") / F.nullif(F.col("distinct_ids_rank"), F.lit(0)), 3).alias("n_rows_per_distinct_ids_rank"),
    )
    .orderBy("second_first_reported_at")
)
