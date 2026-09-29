from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_month_by_premium_bu = Window.partitionBy(F.date_trunc("month", "end_date")).orderBy(F.col("premium_bucket").asc_nulls_last())
w_first_claim_reported_at_mont = Window.partitionBy(F.date_trunc("month", "first_claim_reported_at"), "holder_id")
w_end_date_month_by_premium_bu_2 = Window.partitionBy(F.date_trunc("month", "end_date")).orderBy(F.col("premium_bucket").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_latest = Window.partitionBy("product").orderBy(F.col("min_premium_so_far").desc(), F.col("id").desc())

cleaned_policys = (
    df_policies
    .filter(F.col("product").isin("TRAVEL", "HEALTH"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.initcap(F.trim(F.col("product"))).alias("product_clean"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.length(F.trim(F.col("product"))).alias("product_len"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("approved_amount").alias("max_claim_approved_amount"),
        F.min("claimed_amount").alias("min_claim_claimed_amount"),
        F.min("reported_at").alias("first_claim_reported_at"),
    )
)

with_counts = (
    cleaned_policys.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.max_claim_approved_amount",
        "c.min_claim_claimed_amount",
        "c.first_claim_reported_at",
    )
)

with_activity = (
    with_counts.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.premium").between(2013.8, 4417.0)) & (F.col("p.status").isNotNull()), "left_semi")
)

with_trends = (
    with_activity
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        "product_clean",
        "product_len",
        "first_claim_reported_at",
        F.rank().over(w_end_date_month_by_premium_bu).alias("premium_bucket_rank"),
        F.size(F.collect_set("status").over(w_first_claim_reported_at_mont)).alias("distinct_status_in_first_claim_reported_at_month"),
        F.min("premium").over(w_end_date_month_by_premium_bu_2).alias("min_premium_so_far"),
        F.round(F.col("premium_bucket") - F.avg("premium_bucket").over(w_first_claim_reported_at_mont), 2).alias("premium_bucket_vs_first_claim_reported_at_month_"),
    )
)

unique_rows = (
    with_trends
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

without_matches = (
    unique_rows.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.status").isin("OPEN", "IN_REVIEW")) & (F.col("c.claim_date").isNotNull()), "left_anti")
)

with_activity_2 = (
    without_matches
    .crossJoin(without_matches.agg(F.max("premium_bucket_vs_first_claim_reported_at_month_").alias("overall_max_premium_bucket_vs_first_claim_report")))
    .filter(F.col("premium_bucket_vs_first_claim_reported_at_month_") < F.col("overall_max_premium_bucket_vs_first_claim_report"))
    .drop("overall_max_premium_bucket_vs_first_claim_report")
)

result = (
    with_activity_2
    .rollup("product_len", "product", "status")
    .agg(
        F.sum("product_len").alias("total_product_len"),
        F.round(F.avg("premium"), 2).alias("avg_premium"),
        F.round(100 * F.count(F.when(F.col("status") == "LAPSED", 1)) / F.count("*"), 1).alias("pct_status_lapsed"),
    )
    .select(
        "product_len",
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "total_product_len",
        "avg_premium",
        "pct_status_lapsed",
    )
    .orderBy(
        F.col("product_len").asc_nulls_last(),
        F.col("product").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
    )
)
