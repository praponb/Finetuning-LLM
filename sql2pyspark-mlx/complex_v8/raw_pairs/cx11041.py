from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_status_by_report = Window.partitionBy("adjuster_id", "status").orderBy("reported_at", "id")
w_reported_at_year_claim_type = Window.partitionBy(F.year("reported_at"), "claim_type")

filtered = (
    df_claims
    .filter(F.col("claim_date") >= F.lit("2024-03-24").cast("date"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
    )
)

windowed = (
    filtered
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.lead("claimed_amount").over(w_adjuster_id_status_by_report).alias("next_claimed_amount"),
        F.max("approved_amount").over(w_reported_at_year_claim_type).alias("reported_at_year_claim_type_max_approved_amount"),
        F.size(F.collect_set("policy_id").over(w_reported_at_year_claim_type)).alias("distinct_policy_id_in_reported_at_year_claim_typ"),
    )
)

qualified = (
    windowed.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.policy_id") <= 5), "left_semi")
)

stats = (
    qualified
    .groupBy("distinct_policy_id_in_reported_at_year_claim_typ", "adjuster_id")
    .agg(
        F.max("next_claimed_amount").alias("max_next_claimed_amount"),
        F.sum(F.when(F.col("status") != "PAID", F.col("reported_at_year_claim_type_max_approved_amount"))).alias("reported_at_year_claim_type_max_approved_amount_"),
        F.count(F.when(F.col("claim_type").isin("theft", "fire", "flood"), 1)).alias("n_claim_type_in_theft_fire_flo"),
        F.avg("distinct_policy_id_in_reported_at_year_claim_typ").alias("avg_distinct_policy_id_in_reported_at_year_claim"),
    )
    .filter(F.col("avg_distinct_policy_id_in_reported_at_year_claim") > 2)
    .drop("avg_distinct_policy_id_in_reported_at_year_claim")
)

screened = (
    stats.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claimed_amount") < 41760.8) & (F.col("c.status").isin("APPROVED", "OPEN", "IN_REVIEW")), "left_anti")
)

exploded_detail_claims = (
    screened.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.approved_amount") >= 7204.1), "inner")
    .select(
        "s.distinct_policy_id_in_reported_at_year_claim_typ",
        "s.adjuster_id",
        "s.max_next_claimed_amount",
        "s.reported_at_year_claim_type_max_approved_amount_",
        "s.n_claim_type_in_theft_fire_flo",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        "c.claim_type",
    )
)

result = (
    exploded_detail_claims
    .cube("claim_type", "n_claim_type_in_theft_fire_flo")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.max("claim_reported_at").alias("last_claim_reported_at"),
        F.countDistinct("claim_id").alias("distinct_claim_ids"),
        F.round(F.avg("n_claim_type_in_theft_fire_flo"), 2).alias("avg_n_claim_type_in_theft_fire_flo"),
    )
    .orderBy(
        F.col("claim_type").asc_nulls_last(),
        F.col("n_claim_type_in_theft_fire_flo").asc_nulls_last(),
    )
)
