from pyspark.sql import functions as F

matched = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 60)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_60d"),
        F.sum("b.approved_amount").alias("prior_approved_amount_60d"),
    )
)

derived = (
    matched
    .select(
        "id",
        "policy_id",
        "prior_rows_60d",
        "prior_approved_amount_60d",
        F.date_format(F.col("claim_date"), "yyyy-MM-dd").alias("claim_date_label"),
        F.abs(F.col("prior_rows_60d") - 6).alias("prior_rows_60d_distance"),
    )
)

result = (
    derived
    .select(
        "id",
        F.col("prior_rows_60d_distance").alias("m_prior_rows_60d_distance"),
        F.col("prior_rows_60d").alias("prior_rows_60d_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_prior_rows_60d_distance", "prior_rows_60d_v"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "metric")
)
