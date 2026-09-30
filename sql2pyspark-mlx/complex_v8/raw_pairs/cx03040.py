from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claimed_amount_full = Window.orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reported_at_month_by_claimed = Window.partitionBy(F.date_trunc("month", "reported_at")).orderBy("claimed_amount", "id")

cleaned = (
    df_claims
    .filter(~F.col("claim_type").isin("medical", "theft"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.date_format(F.col("claim_date"), "MMM yyyy").alias("claim_date_label"),
    )
)

ranked = (
    cleaned
    .withColumn("first_approved_amount", F.first("approved_amount").over(w_all_by_claimed_amount_full))
    .withColumn("claimed_amount_change", F.col("claimed_amount") - F.lag("claimed_amount").over(w_reported_at_month_by_claimed))
)

with_activity = (
    ranked.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.status").isin("IN_REVIEW", "OPEN")) & (F.col("c.policy_id") <= 9), "left_anti")
    .filter(F.col("claim_date_label").isNotNull())
)

derived = (
    with_activity
    .select(
        "*",
        F.concat(F.col("claim_date_label"), F.lit("#"), F.col("policy_id").cast("string")).alias("claim_date_label_ref"),
        (F.floor(F.col("claimed_amount_change") / 50000) * 50000).alias("claimed_amount_change_bucket"),
    )
)

rollup_base = (
    derived
    .groupBy(
        F.date_format("claim_date", "yyyy-MM").alias("claim_date_ym"),
        F.year("reported_at").alias("reported_at_year"),
    )
    .agg(
        F.min("claim_date").alias("first_claim_date"),
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
    )
)

result = (
    rollup_base
    .filter(F.col("claim_date_ym").isNotNull())
    .select(
        "claim_date_ym",
        "reported_at_year",
        "first_claim_date",
        "distinct_adjuster_ids",
    )
    .orderBy(
        F.col("first_claim_date").asc_nulls_last(),
        F.col("distinct_adjuster_ids").desc(),
    )
)
