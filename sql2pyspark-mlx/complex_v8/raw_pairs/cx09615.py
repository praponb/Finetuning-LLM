from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_status_by_premium = Window.partitionBy("product", "status").orderBy("premium", "id")
w_product_status_by_premium_ti = Window.partitionBy("product", "status").orderBy("premium")

filtered_policys = (
    df_policies
    .filter((F.col("product") != "TRAVEL") | (F.col("end_date").between(F.lit("2026-01-20").cast("date"), F.lit("2027-05-16").cast("date"))))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
    )
)

windowed = (
    filtered_policys
    .withColumn("prev_end_date", F.lag("end_date").over(w_product_status_by_premium))
    .withColumn("premium_rank", F.rank().over(w_product_status_by_premium_ti))
)

detail_rows_claims = (
    windowed.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.status",
        "s.prev_end_date",
        F.col("c.id").alias("claim_id"),
        "c.claim_date",
        "c.claim_type",
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

with_activity = (
    detail_rows_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.reported_at") < F.lit("2025-02-18 09:00:00").cast("timestamp")) & (F.col("c.adjuster_id").isNotNull()), "left_semi")
)

with_activity_2 = (
    with_activity.alias("s")
    .join(df_claims.alias("c"), (F.col("c.id") == F.col("s.claim_id")) & (F.col("c.status") == "PAID") & (F.col("c.reported_at") >= F.lit("2025-01-25 08:00:00").cast("timestamp")), "left_anti")
)

status_stats = (
    with_activity_2
    .groupBy("status")
    .agg(F.max("premium").alias("max_premium"))
)

outliers = (
    with_activity_2.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.premium") == F.col("g.max_premium"))
    .select(
        "s.*",
        "g.max_premium",
        F.round(F.col("s.premium") - F.col("g.max_premium"), 2).alias("premium_minus_max"),
    )
)

with_parent_claim = (
    outliers.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.status",
        "s.prev_end_date",
        "s.claim_id",
        "s.claim_date",
        "s.claim_type",
        "s.claim_approved_amount",
        "s.premium_minus_max",
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

result = (
    with_parent_claim
    .groupingSets(
        [["claim_adjuster_id", "claim_type"], ["claim_adjuster_id"], []],
        "claim_adjuster_id", "claim_type",
    )
    .agg(
        F.max("claim_date").alias("last_claim_date"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
    )
    .select(
        "claim_adjuster_id",
        F.coalesce(F.col("claim_type"), F.lit("ALL")).alias("claim_type"),
        "last_claim_date",
        "distinct_holder_ids",
    )
)
