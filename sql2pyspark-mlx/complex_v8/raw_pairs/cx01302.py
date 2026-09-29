from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_status_claim_type_by_c = Window.partitionBy("claim_status", "claim_type").orderBy(F.col("claim_claimed_amount").asc_nulls_last(), "id", "claim_id").rowsBetween(-6, Window.currentRow)
w_claim_type_claim_adjuster_id = Window.partitionBy("claim_type", "claim_adjuster_id").orderBy("claim_claimed_amount", "id", "claim_id")
w_adjuster_id = Window.partitionBy("adjuster_id")
w_claim_type_claim_adjuster_id_2 = Window.partitionBy("claim_type", "claim_adjuster_id").orderBy("claim_claimed_amount")

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
        "c.adjuster_id",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

with_children_claims = (
    matched.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.product",
        F.col("c.id").alias("claim_id"),
        "c.claim_type",
        F.col("c.status").alias("claim_status"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

with_windows = (
    with_children_claims
    .select(
        "*",
        F.sum("claim_claimed_amount").over(w_claim_status_claim_type_by_c).alias("claim_claimed_amount_sum_7"),
        F.ntile(10).over(w_claim_type_claim_adjuster_id).alias("claim_claimed_amount_decile"),
        F.count(F.lit(1)).over(w_adjuster_id).alias("rows_in_adjuster_id"),
        F.round(F.col("claim_claimed_amount") - F.avg("claim_claimed_amount").over(w_adjuster_id), 2).alias("claim_claimed_amount_vs_adjuster_id_avg"),
        F.round(F.cume_dist().over(w_claim_type_claim_adjuster_id_2), 3).alias("claim_claimed_amount_cume_dist"),
    )
)

result = (
    with_windows
    .groupBy("claim_claimed_amount_decile")
    .pivot("claim_type", ["delay", "collision"])
    .agg(F.max("rows_in_adjuster_id"))
    .orderBy("claim_claimed_amount_decile")
)
