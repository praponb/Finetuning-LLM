from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_id = Window.partitionBy("claim_id")
w_claim_id_by_claimed_amount_t = Window.partitionBy("claim_id").orderBy(F.col("claimed_amount_to_premium_ratio").asc_nulls_last(), "claim_id")
w_all_by_claimed_amount_to_pre = Window.orderBy("claimed_amount_to_premium_ratio")
w_claim_id_by_claimed_amount_t_2 = Window.partitionBy("claim_id").orderBy(F.col("claimed_amount_to_premium_ratio").asc_nulls_last(), "claim_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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
        "c.status",
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

line_level = (
    covered.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claimed_amount",
        "s.status",
        "s.premium",
        F.col("c.id").alias("claim_id"),
        F.col("c.claim_type").alias("claim_claim_type"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

grouped = (
    line_level
    .groupBy("claim_id")
    .agg(
        F.round(F.sum("claimed_amount"), 2).alias("total_claimed_amount"),
        F.round(F.sum("claimed_amount") / F.nullif(F.sum("premium"), F.lit(0)), 3).alias("claimed_amount_to_premium_ratio"),
    )
)

with_windows = (
    grouped
    .select(
        "*",
        F.count(F.lit(1)).over(w_claim_id).alias("rows_in_claim_id"),
        F.ntile(10).over(w_claim_id_by_claimed_amount_t).alias("claimed_amount_to_premium_ratio_decile"),
        F.round(F.percent_rank().over(w_all_by_claimed_amount_to_pre), 3).alias("claimed_amount_to_premium_ratio_percent_rank"),
        F.max("total_claimed_amount").over(w_claim_id_by_claimed_amount_t_2).alias("max_total_claimed_amount_so_far"),
    )
)

result = (
    with_windows
    .filter(F.col("claimed_amount_to_premium_ratio_percent_rank").between(0.1, 0.6))
    .select(
        "claim_id",
        "total_claimed_amount",
        "claimed_amount_to_premium_ratio",
        "rows_in_claim_id",
        "claimed_amount_to_premium_ratio_decile",
        "max_total_claimed_amount_so_far",
    )
    .orderBy(
        "claimed_amount_to_premium_ratio",
        F.col("max_total_claimed_amount_so_far").desc(),
    )
)
