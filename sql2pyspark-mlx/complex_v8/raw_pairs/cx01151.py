from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_claim_date_year = Window.partitionBy("policy_id").orderBy(F.col("claim_date_year").desc())
w_claim_date_year = Window.partitionBy("claim_date_year")
w_policy_id_by_claim_date_year_2 = Window.partitionBy("policy_id").orderBy(F.col("claim_date_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_by_claim_date_year_3 = Window.partitionBy("policy_id").orderBy(F.col("claim_date_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_claims = (
    df_claims
    .filter((F.col("approved_amount").between(0.0, 25402.3)) & (F.col("policy_id") <= 29))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        F.round(F.col("claimed_amount") * 1.5, 2).alias("claimed_amount_adj"),
    )
)

qualified = (
    base_claims.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.product").isin("TRAVEL", "AUTO")), "left_anti")
)

tagged = qualified.withColumn("claim_date_year", F.year(F.col("claim_date")))

windowed = (
    tagged
    .select(
        "*",
        F.round(F.percent_rank().over(w_policy_id_by_claim_date_year), 3).alias("claim_date_year_percent_rank"),
        F.round(F.cume_dist().over(w_policy_id_by_claim_date_year), 3).alias("claim_date_year_cume_dist"),
        F.max("claimed_amount_adj").over(w_claim_date_year).alias("claim_date_year_max_claimed_amount_adj"),
        F.first("claimed_amount").over(w_policy_id_by_claim_date_year_2).alias("first_claimed_amount"),
        F.last("claim_date_year", ignorenulls=True).over(w_policy_id_by_claim_date_year_3).alias("last_known_claim_date_year"),
        F.size(F.collect_set("status").over(w_claim_date_year)).alias("distinct_status_in_claim_date_year"),
    )
)

with_details_policy = (
    windowed.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.claimed_amount_adj",
        "s.claim_date_year_cume_dist",
        "s.claim_date_year_max_claimed_amount_adj",
        "s.first_claimed_amount",
        "s.last_known_claim_date_year",
        "s.distinct_status_in_claim_date_year",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("a.level").alias("adjuster_level"),
    )
)

screened = (
    with_details_policy.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.policy_holder_id")) & (F.col("p.premium") <= 3817.5) & (F.col("p.product") == "LIFE"), "left_anti")
    .filter(F.col("claimed_amount_adj").between(22570.0, 60020.0))
)

result = (
    screened
    .select(
        "id",
        "policy_id",
        "claim_type",
        "claimed_amount",
        "claimed_amount_adj",
        "claim_date_year_cume_dist",
        "claim_date_year_max_claimed_amount_adj",
        "first_claimed_amount",
        "distinct_status_in_claim_date_year",
        "policy_holder_id",
    )
    .orderBy(F.col("claimed_amount").asc_nulls_last())
)
