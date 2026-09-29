from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_pct_status_is_not_nul = Window.orderBy(F.col("pct_status_is_not_null").asc_nulls_last(), "holder_id", "claim_adjuster_id", "status")
w_n_status_expired_status_by_p = Window.partitionBy("n_status_expired", "status").orderBy("pct_status_is_not_null")
w_n_status_expired_status_by_p_2 = Window.partitionBy("n_status_expired", "status").orderBy("pct_status_is_not_null", "holder_id", "claim_adjuster_id", "status")

recent = (
    df_policies
    .filter(F.col("status").isin("ACTIVE", "CANCELLED"))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
        F.abs(F.col("premium") - 3521.8).alias("premium_distance"),
    )
)

qualified = (
    recent.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.premium") > 2289.0) & (F.col("p.status").isin("LAPSED", "ACTIVE", "CANCELLED")), "left_semi")
    .filter(F.year(F.col("start_date")) == 2025)
)

screened = (
    qualified.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.year(F.col("p.end_date")) == 2026), "left_anti")
)

detail_rows_claims = (
    screened.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.status",
        "s.premium_distance",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

grouped = (
    detail_rows_claims
    .filter(F.col("holder_id") <= 17)
    .groupBy("holder_id", "claim_adjuster_id", "status")
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.round(100 * F.count(F.when(F.col("status").isNotNull(), 1)) / F.count("*"), 1).alias("pct_status_is_not_null"),
        F.sum(F.when(F.col("status") == "EXPIRED", 1).otherwise(0)).alias("n_status_expired"),
    )
)

metrics = (
    grouped
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_pct_status_is_not_nul), 3).alias("pct_status_is_not_null_cume_dist"),
        F.ntile(10).over(w_all_by_pct_status_is_not_nul).alias("pct_status_is_not_null_decile"),
        (F.col("pct_status_is_not_null") - F.lag("pct_status_is_not_null").over(w_all_by_pct_status_is_not_nul)).alias("pct_status_is_not_null_change"),
        F.rank().over(w_n_status_expired_status_by_p).alias("pct_status_is_not_null_rank"),
        F.ntile(5).over(w_n_status_expired_status_by_p_2).alias("pct_status_is_not_null_quintile"),
    )
)

without_matches = (
    metrics.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.claim_adjuster_id")) & (F.col("c.claimed_amount").between(15381.6, 44921.3)) & (F.col("c.claim_date").isNotNull()), "left_anti")
)

result = (
    without_matches
    .select(
        "holder_id",
        "claim_adjuster_id",
        "pct_status_is_not_null_rank",
        "n_status_expired",
        "pct_status_is_not_null_quintile",
    )
    .unpivot(
        ids=["holder_id", "claim_adjuster_id"],
        values=["pct_status_is_not_null_rank", "n_status_expired", "pct_status_is_not_null_quintile"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("holder_id", "claim_adjuster_id", "field")
)
