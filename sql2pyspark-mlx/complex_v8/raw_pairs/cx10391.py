from pyspark.sql import functions as F

base_policys = (
    df_policies
    .filter((F.col("status") == "LAPSED") & (F.col("end_date") >= F.lit("2025-10-12").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        F.concat(F.col("status"), F.lit("#"), F.col("holder_id").cast("string")).alias("status_ref"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
)

enriched_holder = (
    base_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.risk_score") > 0.1)
    .select("s.*", F.col("p.risk_score").alias("holder_risk_score"))
)

result = (
    enriched_holder
    .groupBy("end_date", "holder_id")
    .pivot("status", ["ACTIVE", "LAPSED"])
    .agg(F.min("premium_bucket"))
    .orderBy("end_date", "holder_id")
)
