from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_policy_id_by_claimed = Window.partitionBy("product", "policy_id").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_adjuster_id_by_app = Window.partitionBy("policy_id", "adjuster_id").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(-1, -1)

covered = (
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
        "c.adjuster_id",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

joined = (
    covered.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.adjuster_id",
        "s.approved_amount",
        "s.product",
        "a.adjuster_name",
    )
)

with_windows = (
    joined
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "adjuster_id",
        "approved_amount",
        "product",
        "adjuster_name",
        F.last("claimed_amount").over(w_product_policy_id_by_claimed).alias("last_claimed_amount"),
        F.max("approved_amount").over(w_policy_id_adjuster_id_by_app).alias("approved_amount_max_1_prior"),
    )
)

rollup_base = (
    with_windows
    .groupBy("product", "adjuster_id", "policy_id")
    .agg(
        F.round(F.sum("approved_amount_max_1_prior") / F.nullif(F.sum("claimed_amount"), F.lit(0)), 3).alias("approved_amount_max_1_prior_to_claimed_amount_ra"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.max("approved_amount_max_1_prior").alias("max_approved_amount_max_1_prior"),
        F.sum(F.when(F.col("adjuster_name").isNotNull(), F.col("claimed_amount")).otherwise(0)).alias("claimed_amount_adjuster_name_is_not_null"),
        F.round(F.sum("last_claimed_amount") / F.nullif(F.sum("approved_amount"), F.lit(0)), 3).alias("last_claimed_amount_to_approved_amount_ratio"),
        F.avg("approved_amount_max_1_prior").alias("avg_approved_amount_max_1_prior_check"),
    )
    .filter(F.col("avg_approved_amount_max_1_prior_check") > 15000.0)
    .drop("avg_approved_amount_max_1_prior_check")
)

result = (
    rollup_base
    .filter(F.col("approved_amount_max_1_prior_to_claimed_amount_ra") > 4.0)
    .select(
        "product",
        "policy_id",
        "approved_amount_max_1_prior_to_claimed_amount_ra",
        "product_list",
        "claimed_amount_adjuster_name_is_not_null",
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
        F.size(F.col("product_list")).alias("product_list_count"),
    )
)
