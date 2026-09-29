from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_policy_id_by_prem = Window.partitionBy("claim_type", "policy_id").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_type_by_claim_date_mov = Window.partitionBy("claim_type").orderBy(F.col("claim_date").desc(), "id").rowsBetween(-2, Window.currentRow)
w_claim_type_by_claim_date_tie = Window.partitionBy("claim_type").orderBy(F.col("claim_date").desc())
w_claim_type_policy_id_by_prem_2 = Window.partitionBy("claim_type", "policy_id").orderBy("premium", "id")
w_claim_type_policy_id_by_prem_3 = Window.partitionBy("claim_type", "policy_id").orderBy("premium")

joined_ranges = (
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
        "c.approved_amount",
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

with_activity = (
    joined_ranges
    .crossJoin(joined_ranges.agg(F.max("claimed_amount").alias("overall_max_claimed_amount")))
    .filter(F.col("claimed_amount") < F.col("overall_max_claimed_amount"))
    .drop("overall_max_claimed_amount")
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.min("claim_date").alias("first_claim_date"))
)

profiled = (
    with_activity.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.first_claim_date")
)

sequenced = (
    profiled
    .select(
        "*",
        F.nth_value("claimed_amount", 2).over(w_claim_type_policy_id_by_prem).alias("second_claimed_amount"),
        F.max("claimed_amount").over(w_claim_type_by_claim_date_mov).alias("claimed_amount_max_3"),
        F.dense_rank().over(w_claim_type_by_claim_date_tie).alias("claim_date_dense_rank"),
        F.ntile(4).over(w_claim_type_policy_id_by_prem_2).alias("premium_quartile"),
        F.round(F.percent_rank().over(w_claim_type_policy_id_by_prem_3), 3).alias("premium_percent_rank"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "product",
        "premium",
        "second_claimed_amount",
    )
    .orderBy("claim_date", F.col("id").desc())
)
