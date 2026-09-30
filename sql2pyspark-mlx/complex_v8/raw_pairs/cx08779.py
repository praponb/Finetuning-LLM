from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_month_by_claimed = Window.partitionBy(F.month("claim_date")).orderBy(F.col("claimed_amount").desc(), "id")
w_claim_date_month_by_claimed_2 = Window.partitionBy(F.month("claim_date")).orderBy(F.col("claimed_amount").desc())
w_adjuster_id_policy_id_by_max = Window.partitionBy("adjuster_id", "policy_id").orderBy("max_claim_approved_amount", "id")
w_adjuster_id_policy_id_by_max_2 = Window.partitionBy("adjuster_id", "policy_id").orderBy("max_claim_approved_amount")

recent = (
    df_claims
    .filter(F.col("claim_type").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
        F.greatest(F.col("approved_amount"), F.col("claimed_amount")).alias("greatest_approved_amount_claimed_amount"),
    )
)

metrics = (
    recent
    .select(
        "*",
        (F.col("approved_amount") - F.lag("approved_amount").over(w_claim_date_month_by_claimed)).alias("approved_amount_change"),
        F.round(F.cume_dist().over(w_claim_date_month_by_claimed_2), 3).alias("claimed_amount_cume_dist"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("reported_at").alias("last_claim_reported_at"),
        F.max("approved_amount").alias("max_claim_approved_amount"),
    )
)

with_stats = (
    metrics.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.last_claim_reported_at", "c.max_claim_approved_amount")
)

tagged = (
    with_stats
    .select(
        "*",
        F.concat_ws("-", F.col("status"), F.col("claim_type_slug")).alias("status_claim_type_slug_label"),
        F.greatest(F.col("approved_amount"), F.col("approved_amount_change")).alias("greatest_approved_amount_approved_amount_change"),
        F.date_format(F.col("last_claim_reported_at"), "yyyy-MM-dd").alias("last_claim_reported_at_label"),
    )
)

ranked = (
    tagged
    .select(
        "*",
        F.ntile(10).over(w_adjuster_id_policy_id_by_max).alias("max_claim_approved_amount_decile"),
        F.round(F.cume_dist().over(w_adjuster_id_policy_id_by_max_2), 3).alias("max_claim_approved_amount_cume_dist"),
    )
)

status_stats = (
    ranked
    .groupBy("status")
    .agg(F.max("greatest_approved_amount_claimed_amount").alias("max_greatest_approved_amount_claimed_amount"))
)

compared = (
    ranked.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.greatest_approved_amount_claimed_amount") == F.col("g.max_greatest_approved_amount_claimed_amount"))
    .select(
        "s.*",
        "g.max_greatest_approved_amount_claimed_amount",
        F.round(F.col("s.greatest_approved_amount_claimed_amount") - F.col("g.max_greatest_approved_amount_claimed_amount"), 2).alias("greatest_approved_amount_claimed_amount_minus_ma"),
    )
)

result = (
    compared
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "claim_type_slug",
        "approved_amount_change",
        "claimed_amount_cume_dist",
        "max_claim_approved_amount",
        "status_claim_type_slug_label",
        "greatest_approved_amount_approved_amount_change",
        "max_greatest_approved_amount_claimed_amount",
        "greatest_approved_amount_claimed_amount_minus_ma",
    )
    .orderBy(F.col("greatest_approved_amount_approved_amount_change").desc())
)
