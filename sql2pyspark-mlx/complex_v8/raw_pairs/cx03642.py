from pyspark.sql import functions as F

valid = (
    df_policies
    .filter(F.col("holder_id") >= 2)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.date_trunc("week", F.col("end_date")).alias("end_date_week"),
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
        F.add_months(F.col("end_date"), 1).alias("end_date_plus_1m"),
    )
)

with_details_holder = (
    valid.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.risk_score").between(0.2, 0.6))
    .select(
        "s.*",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.segment").alias("holder_segment"),
    )
)

result = (
    with_details_holder
    .groupBy("product", "holder_id")
    .pivot("holder_segment", ["SME", "RETAIL"])
    .agg(F.sum("holder_risk_score"))
)
