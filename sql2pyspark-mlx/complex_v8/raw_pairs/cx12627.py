from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_n_rows = Window.orderBy(F.col("n_rows").desc(), "premium_tier")
w_premium_tier = Window.partitionBy("premium_tier")
w_n_rows_by_n_rows_ties = Window.partitionBy("n_rows").orderBy("n_rows")

prepared = (
    df_policies
    .filter((F.col("status").isNotNull()) & (F.col("start_date") >= F.lit("2024-06-19").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.when(F.col("premium") >= 3532.7, "HIGH").when(F.col("premium") >= 2013.8, "MEDIUM").otherwise("LOW").alias("premium_tier"),
    )
)

without_matches = (
    prepared.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_date").between(F.lit("2024-06-26").cast("date"), F.lit("2025-04-20").cast("date"))) & (F.col("c.approved_amount") <= 32510.7), "left_anti")
)

with_details = (
    without_matches.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.status",
        "s.premium_filled",
        "s.premium_tier",
        F.col("p.risk_score").alias("holder_risk_score"),
        "p.holder_name",
    )
)

rollup_base = (
    with_details
    .groupBy("premium_tier")
    .agg(
        F.array_sort(F.collect_set("holder_name")).alias("holder_name_list"),
        F.count("*").alias("n_rows"),
    )
)

windowed = (
    rollup_base
    .select(
        "*",
        F.row_number().over(w_all_by_n_rows).alias("rn"),
        F.count(F.lit(1)).over(w_premium_tier).alias("rows_in_premium_tier"),
        F.round(F.col("n_rows") / F.nullif(F.sum("n_rows").over(w_premium_tier), F.lit(0)), 4).alias("n_rows_share_of_premium_tier"),
        F.round(F.cume_dist().over(w_n_rows_by_n_rows_ties), 3).alias("n_rows_cume_dist"),
    )
)

result = (
    windowed
    .select(
        "premium_tier",
        "holder_name_list",
        "n_rows",
        "rn",
        "n_rows_share_of_premium_tier",
        F.length(F.trim(F.col("premium_tier"))).alias("premium_tier_len"),
    )
    .orderBy(
        F.col("rn").desc_nulls_first(),
        F.col("n_rows_share_of_premium_tier").desc(),
    )
)
