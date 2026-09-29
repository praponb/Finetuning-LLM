from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_reported_at_ties = Window.orderBy("reported_at")
w_all_by_reported_at = Window.orderBy(F.col("reported_at").desc(), "id")

valid = (
    df_claims
    .filter(F.col("approved_amount") <= 17759.3)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.when(F.col("approved_amount") >= 15477.7, "A").when(F.col("approved_amount") >= 1273.7, "B").otherwise("C").alias("approved_amount_tier"),
        (F.floor(F.col("approved_amount") / 10000) * 10000).alias("approved_amount_bucket"),
    )
)

combined_details = (
    valid.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
        F.col("p.product").alias("policy_product"),
        F.col("p.premium").alias("policy_premium"),
    )
)

with_trends = (
    combined_details
    .select(
        "id",
        "policy_id",
        "approved_amount",
        "status",
        "reported_at",
        "adjuster_team",
        "policy_product",
        F.rank().over(w_all_by_reported_at_ties).alias("reported_at_rank"),
        (F.col("approved_amount") - F.lag("approved_amount").over(w_all_by_reported_at)).alias("approved_amount_change"),
    )
)

annotated = (
    with_trends
    .filter(F.col("reported_at") < F.lit("2025-02-18 09:00:00").cast("timestamp"))
    .select(
        "id",
        "policy_id",
        "approved_amount",
        "reported_at",
        "policy_product",
        "reported_at_rank",
        "approved_amount_change",
        F.round(F.col("approved_amount_change") * 1.1, 2).alias("approved_amount_change_adj"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
    )
)

result = (
    annotated
    .groupingSets(
        [["policy_product", "policy_id", "reported_at_rank"], ["policy_product"], ["policy_id"]],
        "policy_product", "policy_id", "reported_at_rank",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(
        F.col("policy_product").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
        F.col("reported_at_rank").asc_nulls_last(),
    )
)
