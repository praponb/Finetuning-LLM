from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_product_by_claim_d = Window.partitionBy("policy_id", "product").orderBy(F.col("claim_date").asc_nulls_last(), "id")
w_policy_id = Window.partitionBy("policy_id")
w_policy_id_by_claim_date_ties = Window.partitionBy("policy_id").orderBy(F.col("claim_date").asc_nulls_last())
w_policy_id_by_claim_date_movi = Window.partitionBy("policy_id").orderBy(F.col("claim_date").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)

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
        "c.approved_amount",
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

ranked = (
    covered
    .select(
        "id",
        "policy_id",
        "claim_date",
        "approved_amount",
        "product",
        "premium",
        F.ntile(4).over(w_policy_id_product_by_claim_d).alias("claim_date_quartile"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_policy_id), F.lit(0)), 4).alias("premium_share_of_policy_id"),
        F.count(F.lit(1)).over(w_policy_id).alias("rows_in_policy_id"),
        F.round(F.cume_dist().over(w_policy_id_by_claim_date_ties), 3).alias("claim_date_cume_dist"),
        F.round(F.avg("premium").over(w_policy_id_by_claim_date_movi), 2).alias("premium_avg_2"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.max("approved_amount").alias("max_claim_approved_amount"))
)

summarized_rows = (
    ranked.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.max_claim_approved_amount")
)

result = (
    summarized_rows
    .select(
        "id",
        "policy_id",
        "product",
        "premium",
        "claim_date_quartile",
        "rows_in_policy_id",
        "premium_avg_2",
        "max_claim_approved_amount",
    )
    .orderBy(F.col("product").asc_nulls_last())
)
