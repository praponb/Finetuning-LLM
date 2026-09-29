from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claim_claim_type_coun = Window.orderBy("claim_claim_type_count", "claim_date_ym", "policy_id", "product")
w_all_by_claim_claim_type_coun_2 = Window.orderBy("claim_claim_type_count", "claim_date_ym", "policy_id", "product").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

covered = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "inner",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.approved_amount",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

stats = (
    covered
    .groupBy(
        F.date_format("claim_date", "yyyy-MM").alias("claim_date_ym"),
        "policy_id",
        "product",
    )
    .agg(
        F.count(F.when(F.col("product") != "HOME", 1)).alias("n_product_home"),
        F.sum(F.when(~F.col("product").isin("TRAVEL", "HEALTH"), F.col("claimed_amount"))).alias("claimed_amount_product_not_in_travel_health"),
        F.count(F.when(F.col("product").isin("HOME", "HEALTH"), 1)).alias("n_product_in_home_health"),
        F.max("approved_amount").alias("max_approved_amount_check"),
    )
    .filter(F.col("max_approved_amount_check") > 0.0)
    .drop("max_approved_amount_check")
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.round(F.avg("approved_amount"), 2).alias("avg_claim_approved_amount"),
        F.count(F.when(F.col("claim_type").isNotNull(), 1)).alias("claim_claim_type_count"),
        F.count("*").alias("claim_count"),
    )
)

summarized_rows = (
    stats.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "c.avg_claim_approved_amount",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

sequenced = (
    summarized_rows
    .select(
        "*",
        F.ntile(4).over(w_all_by_claim_claim_type_coun).alias("claim_claim_type_count_quartile"),
        F.nth_value("avg_claim_approved_amount", 2).over(w_all_by_claim_claim_type_coun_2).alias("second_avg_claim_approved_amount"),
    )
)

result = (
    sequenced
    .filter(F.col("second_avg_claim_approved_amount") > 17500.0)
    .select(
        "claim_date_ym",
        "policy_id",
        "n_product_home",
        "n_product_in_home_health",
        "avg_claim_approved_amount",
        "claim_claim_type_count",
        "claim_count",
        "second_avg_claim_approved_amount",
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
        F.concat_ws("-", F.col("claim_date_ym"), F.col("product")).alias("claim_date_ym_product_label"),
    )
)
