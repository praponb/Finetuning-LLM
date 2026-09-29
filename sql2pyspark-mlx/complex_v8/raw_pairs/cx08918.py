from pyspark.sql import functions as F

recent_policys = (
    df_policies
    .filter((F.col("premium") > 1541.8) | (F.col("holder_id").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.lower(F.trim(F.col("product"))).alias("product_clean"),
    )
)

with_details_holder = (
    recent_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.risk_score") < 0.6)
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.status",
        "s.premium_filled",
        "s.product_clean",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

result = (
    with_details_holder
    .select("id", "holder_risk_score", "premium_filled")
    .unpivot(
        ids=["id"],
        values=["holder_risk_score", "premium_filled"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
