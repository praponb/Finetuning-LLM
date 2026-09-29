from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("status").orderBy(F.col("reported_at_plus_2h").desc(), F.col("id").desc())

prepared_claims = (
    df_claims
    .filter((F.col("policy_id") <= 16) | (F.col("claim_date") >= F.lit("2024-08-01").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        "reported_at",
        F.when(F.col("approved_amount") >= 18478.3, "large").when(F.col("approved_amount") >= 4234.9, "medium").otherwise("small").alias("approved_amount_tier"),
        (F.col("reported_at") + F.expr("INTERVAL 2 HOURS")).alias("reported_at_plus_2h"),
        F.concat(F.col("status"), F.lit("#"), F.col("policy_id").cast("string")).alias("status_ref"),
    )
)

latest = (
    prepared_claims
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

qualified = (
    latest.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left_semi")
    .filter(F.col("status").isNotNull())
)

rollup_base = (
    qualified
    .groupBy(
        "claim_type",
        F.date_format("claim_date", "yyyy-MM").alias("claim_date_ym"),
        "status",
    )
    .agg(
        F.sum(F.when(F.col("status") == "IN_REVIEW", 1).otherwise(0)).alias("n_status_in_review"),
        F.max("reported_at").alias("last_reported_at"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.round(100 * F.count(F.when(F.col("claim_type") != "delay", 1)) / F.count("*"), 1).alias("pct_claim_type_delay"),
        F.min("reported_at").alias("first_reported_at"),
        F.count("*").alias("cnt"),
    )
)

exploded = (
    rollup_base
    .select(
        "claim_type",
        "claim_date_ym",
        "status",
        "n_status_in_review",
        "last_reported_at",
        "pct_claim_type_delay",
        "first_reported_at",
        "cnt",
        F.posexplode("claim_type_list").alias("claim_type_list_item_pos", "claim_type_list_item"),
    )
)

with_activity = (
    exploded
    .crossJoin(exploded.agg(F.min("cnt").alias("overall_min_cnt")))
    .filter(F.col("cnt") > F.col("overall_min_cnt"))
    .drop("overall_min_cnt")
)

claim_type_list_item_stats = (
    with_activity
    .groupBy("claim_type_list_item")
    .agg(F.percentile_approx("n_status_in_review", 0.5).alias("median_n_status_in_review"))
)

above_average = (
    with_activity.alias("s")
    .join(claim_type_list_item_stats.alias("g"), F.col("g.claim_type_list_item") == F.col("s.claim_type_list_item"), "inner")
    .filter(F.col("s.n_status_in_review") >= F.col("g.median_n_status_in_review"))
    .select(
        "s.*",
        "g.median_n_status_in_review",
        F.round(F.col("s.n_status_in_review") - F.col("g.median_n_status_in_review"), 2).alias("n_status_in_review_minus_median"),
    )
)

result = (
    above_average
    .filter(F.col("median_n_status_in_review") >= 4.0)
    .select(
        "claim_type",
        "claim_date_ym",
        "status",
        "n_status_in_review",
        "last_reported_at",
        "pct_claim_type_delay",
        "first_reported_at",
        "cnt",
        "claim_type_list_item_pos",
        "median_n_status_in_review",
        "n_status_in_review_minus_median",
        F.round(F.col("cnt") / F.nullif(F.col("claim_type_list_item_pos"), F.lit(0)), 3).alias("cnt_per_claim_type_list_item_pos"),
    )
    .orderBy(
        F.col("claim_type_list_item_pos").desc(),
        F.col("pct_claim_type_delay").desc(),
    )
)
