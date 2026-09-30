from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_policy_id_by_claim_d = Window.partitionBy("product", "policy_id").orderBy("claim_date", "id").rowsBetween(-2, -1)
w_product_policy_id_by_claim_d_2 = Window.partitionBy("product", "policy_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

matched = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "inner",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.claim_type",
        "c.status",
        "p.product",
        "p.premium",
    )
)

sequenced = (
    matched
    .select(
        "*",
        F.sum("premium").over(w_product_policy_id_by_claim_d).alias("premium_sum_2_prior"),
        F.last("claimed_amount").over(w_product_policy_id_by_claim_d_2).alias("last_claimed_amount"),
    )
)

qualified = (
    sequenced.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.hour(F.col("c.reported_at")).between(6, 10)), "left_anti")
)

result = (
    qualified
    .cube("status", "policy_id")
    .agg(
        F.grouping("status").alias("is_total_status"),
        F.round(F.sum("premium") / F.nullif(F.sum("premium_sum_2_prior"), F.lit(0)), 3).alias("premium_to_premium_sum_2_prior_ratio"),
        F.round(F.sum("premium_sum_2_prior") / F.nullif(F.sum("claimed_amount"), F.lit(0)), 3).alias("premium_sum_2_prior_to_claimed_amount_ratio"),
    )
    .orderBy(F.col("status").asc_nulls_last(), F.col("policy_id").asc_nulls_last())
)
