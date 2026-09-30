from pyspark.sql import functions as F

recent_policys = (
    df_policies
    .filter((F.col("status").isin("EXPIRED", "CANCELLED")) & (F.col("start_date").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.date_trunc("week", F.col("start_date")).alias("start_date_week"),
    )
)

joined = (
    recent_policys.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.year(F.col("p.joined_on")) == 2024), "inner")
    .filter(F.col("p.holder_name").isNotNull())
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.start_date_week",
        F.col("p.risk_score").alias("holder_risk_score"),
        "p.holder_name",
    )
)

with_parent = (
    joined.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.risk_score").between(0.1, 0.6))
    .select("s.*", F.col("p.risk_score").alias("holder_risk_score_2"))
)

result = (
    with_parent
    .select(
        "id",
        "premium",
        "holder_risk_score",
        "holder_risk_score_2",
    )
    .unpivot(
        ids=["id"],
        values=["premium", "holder_risk_score", "holder_risk_score_2"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
