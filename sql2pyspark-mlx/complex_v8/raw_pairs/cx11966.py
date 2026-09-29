from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_group = Window.partitionBy("claim_type_group")
w_claim_type_group_by_pct_clai = Window.partitionBy("claim_type_group").orderBy("pct_claim_type_delay", "claim_type_group").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_pct_claim_type_delay = Window.orderBy(F.col("pct_claim_type_delay").desc(), "claim_type_group")
w_pct_claim_type_delay_quartil = Window.partitionBy("pct_claim_type_delay_quartile").orderBy(F.col("approved_amount_to_claimed_amount_per_approved_a").desc_nulls_first(), "claim_type_group")
w_pct_claim_type_delay_quartil_2 = Window.partitionBy("pct_claim_type_delay_quartile", "claim_type_group")
w_pct_claim_type_delay_quartil_3 = Window.partitionBy("pct_claim_type_delay_quartile", "claim_type_group").orderBy(F.col("pct_claim_type_delay_quartile").desc(), "claim_type_group").rowsBetween(-1, Window.currentRow)
w_pct_claim_type_delay_quartil_4 = Window.partitionBy("pct_claim_type_delay_quartile", "claim_type_group").orderBy(F.col("pct_claim_type_delay_quartile").desc())

recent_claims = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
        F.round(F.col("claimed_amount") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("claimed_amount_per_approved_amount"),
        F.when(F.col("claim_type").isin("flood", "collision"), "group_a").when(F.col("claim_type") == "theft", "group_b").otherwise("other").alias("claim_type_group"),
    )
)

summary = (
    recent_claims
    .groupBy("claim_type_group")
    .agg(
        F.round(F.sum("approved_amount") / F.nullif(F.sum("claimed_amount_per_approved_amount"), F.lit(0)), 3).alias("approved_amount_to_claimed_amount_per_approved_a"),
        F.round(100 * F.count(F.when(F.col("claim_type") == "delay", 1)) / F.count("*"), 1).alias("pct_claim_type_delay"),
    )
)

ranked = (
    summary
    .select(
        "*",
        F.count(F.lit(1)).over(w_claim_type_group).alias("rows_in_claim_type_group"),
        F.nth_value("pct_claim_type_delay", 2).over(w_claim_type_group_by_pct_clai).alias("second_pct_claim_type_delay"),
        F.ntile(4).over(w_all_by_pct_claim_type_delay).alias("pct_claim_type_delay_quartile"),
    )
)

without_matches = (
    ranked
    .crossJoin(ranked.agg(F.max("second_pct_claim_type_delay").alias("overall_max_second_pct_claim_type_delay")))
    .filter(F.col("second_pct_claim_type_delay") < F.col("overall_max_second_pct_claim_type_delay"))
    .drop("overall_max_second_pct_claim_type_delay")
)

ranked_2 = (
    without_matches
    .select(
        "claim_type_group",
        "approved_amount_to_claimed_amount_per_approved_a",
        "pct_claim_type_delay",
        "rows_in_claim_type_group",
        "pct_claim_type_delay_quartile",
        F.row_number().over(w_pct_claim_type_delay_quartil).alias("approved_amount_to_claimed_amount_per_approved_a_2"),
        F.round(F.col("rows_in_claim_type_group") - F.avg("rows_in_claim_type_group").over(w_pct_claim_type_delay_quartil_2), 2).alias("rows_in_claim_type_group_vs_pct_claim_type_delay"),
        F.round(F.avg("second_pct_claim_type_delay").over(w_pct_claim_type_delay_quartil_3), 2).alias("second_pct_claim_type_delay_avg_2"),
        F.dense_rank().over(w_pct_claim_type_delay_quartil_4).alias("pct_claim_type_delay_quartile_dense_rank"),
        F.round(F.col("pct_claim_type_delay") / F.nullif(F.sum("pct_claim_type_delay").over(w_pct_claim_type_delay_quartil_2), F.lit(0)), 4).alias("pct_claim_type_delay_share_of_pct_claim_type_del"),
    )
)

result = (
    ranked_2
    .rollup("claim_type_group", "pct_claim_type_delay_quartile", "pct_claim_type_delay_quartile_dense_rank")
    .agg(
        F.round(F.stddev("rows_in_claim_type_group"), 2).alias("stddev_rows_in_claim_type_group"),
        F.countDistinct("claim_type_group").alias("distinct_claim_type_groups"),
    )
    .orderBy(
        F.col("claim_type_group").asc_nulls_last(),
        F.col("pct_claim_type_delay_quartile").asc_nulls_last(),
        F.col("pct_claim_type_delay_quartile_dense_rank").asc_nulls_last(),
    )
)
