from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_policy_start_date_ful = Window.orderBy("policy_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_policy_start_date = Window.orderBy("policy_start_date", "id")

covered = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "left",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.adjuster_id",
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

with_details = (
    covered.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.team").isNotNull()), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.*",
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

sequenced = (
    with_details
    .withColumn("second_claimed_amount", F.nth_value("claimed_amount", 2).over(w_all_by_policy_start_date_ful))
    .withColumn("next2_claim_date", F.lead("claim_date", 2).over(w_all_by_policy_start_date))
)

screened = (
    sequenced.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left_semi")
    .filter(F.col("next2_claim_date").isNotNull())
)

result = (
    screened
    .groupingSets(
        [["adjuster_id", "policy_id", "claim_type"], ["adjuster_id"], []],
        "adjuster_id", "policy_id", "claim_type",
    )
    .agg(
        F.grouping("adjuster_id").alias("is_total_adjuster_id"),
        F.round(F.avg("policy_premium"), 2).alias("avg_policy_premium"),
        F.sum("claimed_amount").alias("total_claimed_amount"),
        F.countDistinct("claim_type").alias("distinct_claim_types"),
    )
)
