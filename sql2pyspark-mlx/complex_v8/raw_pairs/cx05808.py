from pyspark.sql import functions as F

prepared_policys = (
    df_policies
    .filter((F.col("end_date").between(F.lit("2025-04-17").cast("date"), F.lit("2026-12-05").cast("date"))) & (F.col("premium") <= 3465.4))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

with_parent = (
    prepared_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.status",
        F.col("p.risk_score").alias("holder_risk_score"),
        "p.holder_name",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

result = (
    with_parent
    .groupBy("status")
    .pivot("product", ["HOME", "HEALTH", "AUTO"])
    .agg(F.sum("holder_risk_score"))
)
