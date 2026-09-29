from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_by_claimed_amoun = Window.partitionBy("adjuster_id").orderBy("claimed_amount", "id")
w_product_adjuster_id_by_claim = Window.partitionBy("product", "adjuster_id").orderBy(F.col("claim_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_adjuster_id = Window.partitionBy("product", "adjuster_id")
w_adjuster_id_by_claimed_amoun_2 = Window.partitionBy("adjuster_id").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

in_window = (
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
        "c.status",
        "p.product",
        "p.premium",
    )
)

metrics = (
    in_window
    .select(
        "*",
        F.lead("claim_date", 3).over(w_adjuster_id_by_claimed_amoun).alias("next3_claim_date"),
        F.count(F.lit(1)).over(w_product_adjuster_id_by_claim).alias("running_count"),
        F.round(F.avg("claimed_amount").over(w_product_adjuster_id), 2).alias("product_adjuster_id_avg_claimed_amount"),
        F.max("claimed_amount").over(w_product_adjuster_id).alias("product_adjuster_id_max_claimed_amount"),
        F.first("premium").over(w_adjuster_id_by_claimed_amoun_2).alias("first_premium"),
    )
)

combined_details_adjuster = (
    metrics.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.adjuster_id",
        "s.product",
        "s.premium",
        "s.next3_claim_date",
        "s.running_count",
        "s.product_adjuster_id_avg_claimed_amount",
        "s.product_adjuster_id_max_claimed_amount",
        "s.first_premium",
        "a.adjuster_name",
    )
)

result = (
    combined_details_adjuster
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "adjuster_id",
        "product",
        "premium",
        "running_count",
        "product_adjuster_id_avg_claimed_amount",
        "product_adjuster_id_max_claimed_amount",
    )
    .orderBy(F.col("policy_id").desc())
)
