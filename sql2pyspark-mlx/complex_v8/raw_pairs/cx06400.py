from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_claim_date_full = Window.partitionBy("policy_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_prior_rows_180d = Window.partitionBy("policy_id", "prior_rows_180d")
w_policy_id_by_claim_date_ties = Window.partitionBy("policy_id").orderBy("claim_date")
w_policy_id_by_claim_date = Window.partitionBy("policy_id").orderBy("claim_date", "id")

covered = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 180)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_180d"),
        F.sum("b.approved_amount").alias("prior_approved_amount_180d"),
    )
)

with_trends = (
    covered
    .select(
        "*",
        F.last("claim_date").over(w_policy_id_by_claim_date_full).alias("last_claim_date"),
        F.max("prior_approved_amount_180d").over(w_policy_id_prior_rows_180d).alias("policy_id_prior_rows_180d_max_prior_approved_amo"),
        F.dense_rank().over(w_policy_id_by_claim_date_ties).alias("claim_date_dense_rank"),
        F.round(F.cume_dist().over(w_policy_id_by_claim_date_ties), 3).alias("claim_date_cume_dist"),
        F.nth_value("prior_rows_180d", 2).over(w_policy_id_by_claim_date_full).alias("second_prior_rows_180d"),
        F.lead("claim_date", 3).over(w_policy_id_by_claim_date).alias("next3_claim_date"),
    )
)

first_rows = with_trends.filter(F.col("claim_date_dense_rank") == 1)

with_parent = (
    first_rows.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .filter(~F.col("p.product").isin("HEALTH", "HOME"))
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.prior_rows_180d",
        "s.prior_approved_amount_180d",
        "s.last_claim_date",
        "s.policy_id_prior_rows_180d_max_prior_approved_amo",
        "s.claim_date_cume_dist",
        F.col("p.product").alias("policy_product"),
        F.col("po.risk_score").alias("holder_risk_score"),
        F.col("po.region").alias("holder_region"),
    )
)

result = (
    with_parent
    .select(
        "id",
        F.col("holder_risk_score").alias("m_holder_risk_score"),
        F.col("policy_id_prior_rows_180d_max_prior_approved_amo").alias("policy_id_prior_rows_180d_max_prior_approved_amo_2"),
        F.col("claim_date_cume_dist").alias("m_claim_date_cume_dist"),
        F.col("prior_approved_amount_180d").alias("prior_approved_amount_180d_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_holder_risk_score", "policy_id_prior_rows_180d_max_prior_approved_amo_2", "m_claim_date_cume_dist", "prior_approved_amount_180d_v"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "metric_name")
)
