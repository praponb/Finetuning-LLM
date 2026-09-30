from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter(F.col("premium").between(1132.1, 4670.9))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.abs(F.col("premium") - 2033.8).alias("premium_distance"),
        F.length(F.trim(F.col("product"))).alias("product_len"),
    )
)

enriched = (
    cleaned.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        "s.status",
        "s.premium_distance",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

result = (
    enriched
    .select("id", "holder_risk_score", "premium_distance")
    .unpivot(
        ids=["id"],
        values=["holder_risk_score", "premium_distance"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "measure")
)
