from pyspark.sql import functions as F

valid_policys = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("product").isin("HOME", "AUTO"), "group_a").when(F.col("product") == "LIFE", "group_b").otherwise("other").alias("product_group"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date_2"),
    )
)

holder_id_stats = (
    valid_policys
    .groupBy("holder_id")
    .agg(F.percentile_approx("days_start_date_to_end_date", 0.5).alias("median_days_start_date_to_end_date"))
)

outliers = (
    valid_policys.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.days_start_date_to_end_date") >= F.col("g.median_days_start_date_to_end_date"))
    .select(
        "s.*",
        "g.median_days_start_date_to_end_date",
        F.round(F.col("s.days_start_date_to_end_date") - F.col("g.median_days_start_date_to_end_date"), 2).alias("days_start_date_to_end_date_minus_median"),
    )
)

scored = (
    outliers
    .select(
        "*",
        F.concat_ws("-", F.col("status"), F.col("product_group")).alias("status_product_group_label"),
        F.when(F.col("status").isin("EXPIRED", "CANCELLED"), "group_a").when(F.col("status") == "LAPSED", "group_b").otherwise("other").alias("status_group"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
)

with_children_claims = (
    scored.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.status",
        "s.product_group",
        "s.days_start_date_to_end_date",
        "s.days_start_date_to_end_date_2",
        "s.days_start_date_to_end_date_minus_median",
        "s.status_product_group_label",
        "s.status_group",
        "s.premium_bucket",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        "c.claim_date",
    )
)

tagged = (
    with_children_claims
    .filter(F.col("status").isin("ACTIVE", "CANCELLED"))
    .select(
        "id",
        "holder_id",
        "premium",
        "status",
        "product_group",
        "days_start_date_to_end_date",
        "days_start_date_to_end_date_2",
        "status_product_group_label",
        "status_group",
        "claim_id",
        "claim_adjuster_id",
        "claim_date",
        F.date_format(F.col("claim_date"), "MMM yyyy").alias("claim_date_label"),
    )
)

result = (
    tagged
    .select(
        "id",
        "holder_id",
        "premium",
        "product_group",
        "days_start_date_to_end_date",
        "days_start_date_to_end_date_2",
        "status_product_group_label",
        "status_group",
        "claim_id",
        "claim_adjuster_id",
        "claim_date",
        "claim_date_label",
        F.date_format(F.col("claim_date"), "yyyy-MM").alias("claim_date_label_2"),
        F.round(F.col("premium") * 1.5, 2).alias("premium_adj"),
    )
    .orderBy(F.col("id").desc(), F.col("premium").desc())
)
