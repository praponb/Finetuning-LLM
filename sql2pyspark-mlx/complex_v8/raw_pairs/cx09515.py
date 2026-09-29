from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claimed_amount = Window.orderBy("claimed_amount", "id")
w_policy_id_by_claimed_amount = Window.partitionBy("policy_id").orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_product_claim_date_mo = Window.partitionBy("policy_product", F.month("claim_date"))
w_all_by_claimed_amount_2 = Window.orderBy(F.col("claimed_amount").desc(), "id")

valid = df_claims.filter(~F.col("claim_type").isin("medical", "delay"))

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.max("reported_at").alias("last_claim_reported_at"),
        F.min("reported_at").alias("first_claim_reported_at"),
    )
)

summarized_rows = (
    valid.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "c.last_claim_reported_at", "c.first_claim_reported_at")
)

scored = (
    summarized_rows
    .filter(F.col("claim_date") >= F.lit("2024-04-07").cast("date"))
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.concat(F.col("status"), F.lit("#"), F.col("policy_id").cast("string")).alias("status_ref"),
        F.abs(F.col("approved_amount") - 10355.0).alias("approved_amount_distance"),
    )
)

screened = (
    scored.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claim_type") == "flood"), "left_anti")
)

with_parent_policy = (
    screened.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.status_ref",
        F.col("p.product").alias("policy_product"),
        F.col("p.status").alias("policy_status"),
    )
)

with_windows = (
    with_parent_policy
    .select(
        "*",
        F.ntile(4).over(w_all_by_claimed_amount).alias("claimed_amount_quartile"),
        F.sum("claimed_amount").over(w_policy_id_by_claimed_amount).alias("running_claimed_amount"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_policy_product_claim_date_mo), F.lit(0)), 4).alias("claimed_amount_share_of_policy_product_claim_dat"),
        F.lag("claim_date").over(w_all_by_claimed_amount_2).alias("prev_claim_date"),
    )
)

result = (
    with_windows
    .filter(F.col("claim_type") != "medical")
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "policy_product",
        "policy_status",
        "claimed_amount_quartile",
        "running_claimed_amount",
        "prev_claim_date",
    )
    .orderBy(F.col("claim_date").desc(), F.col("policy_product").asc())
)
