from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_claim_id = Window.partitionBy("holder_id", "claim_id")
w_all_by_distinct_ids = Window.orderBy(F.col("distinct_ids").desc(), "claim_id", "status", "holder_id")

prepared_policys = (
    df_policies
    .filter((F.col("status").isin("LAPSED", "ACTIVE")) & (F.col("end_date").isNotNull()))
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
        F.when(F.col("product").isin("LIFE", "HEALTH"), "group_a").when(F.col("product") == "HOME", "group_b").otherwise("other").alias("product_group"),
        F.lower(F.trim(F.col("status"))).alias("status_clean"),
    )
)

with_children_claims = (
    prepared_policys.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.status",
        "s.status_clean",
        F.col("c.id").alias("claim_id"),
        "c.claim_date",
        F.col("c.reported_at").alias("claim_reported_at"),
        "c.claim_type",
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

derived = (
    with_children_claims
    .select(
        "id",
        "holder_id",
        "premium",
        "status",
        "status_clean",
        "claim_id",
        "claim_date",
        "claim_type",
        "claim_approved_amount",
        F.date_format(F.col("claim_date"), "MMM yyyy").alias("claim_date_label"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.upper(F.trim(F.col("status_clean"))).alias("status_clean_clean"),
    )
)

stats = (
    derived
    .filter(F.upper(F.trim(F.col("status_clean_clean"))).like("L%"))
    .groupBy("claim_id", "status", "holder_id")
    .agg(
        F.round(F.sum("claim_approved_amount") / F.nullif(F.sum("premium_filled"), F.lit(0)), 3).alias("claim_approved_amount_to_premium_filled_ratio"),
        F.round(F.stddev("premium"), 2).alias("stddev_premium"),
        F.countDistinct("id").alias("distinct_ids"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.min("claim_approved_amount").alias("min_claim_approved_amount"),
        F.round(F.sum("premium_filled"), 2).alias("total_premium_filled"),
    )
)

with_trends = (
    stats
    .select(
        "*",
        F.round(F.col("total_premium_filled") - F.avg("total_premium_filled").over(w_holder_id_claim_id), 2).alias("total_premium_filled_vs_holder_id_claim_id_avg"),
        F.size(F.collect_set("status").over(w_holder_id_claim_id)).alias("distinct_status_in_holder_id_claim_id"),
        F.row_number().over(w_all_by_distinct_ids).alias("rn"),
    )
)

with_children = (
    with_trends.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "inner")
    .select(
        "s.claim_id",
        "s.status",
        "s.holder_id",
        "s.claim_approved_amount_to_premium_filled_ratio",
        "s.min_claim_approved_amount",
        "s.total_premium_filled",
        "s.distinct_status_in_holder_id_claim_id",
        "s.rn",
        F.col("p.id").alias("policy_id"),
        F.col("p.product").alias("policy_product"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.status").alias("policy_status"),
    )
)

result = (
    with_children
    .rollup("policy_id", "policy_status", "claim_id")
    .agg(
        F.grouping("policy_id").alias("is_total_policy_id"),
        F.count("*").alias("n_rows"),
    )
    .select(
        "policy_id",
        F.coalesce(F.col("policy_status"), F.lit("ALL")).alias("policy_status"),
        "claim_id",
        "is_total_policy_id",
        "n_rows",
    )
    .orderBy(
        F.col("policy_id").asc_nulls_last(),
        F.col("policy_status").asc_nulls_last(),
        F.col("claim_id").asc_nulls_last(),
    )
)
