from pyspark.sql import functions as F

prepared_policys = (
    df_policies
    .filter((F.col("end_date").between(F.lit("2024-11-08").cast("date"), F.lit("2027-03-20").cast("date"))) & (F.col("premium").between(1042.5, 3719.7)))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
    )
)

filtered = (
    prepared_policys.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.start_date").between(F.lit("2024-07-19").cast("date"), F.lit("2025-05-02").cast("date"))), "left_semi")
)

product_stats = (
    filtered
    .groupBy("product")
    .agg(F.avg("premium").alias("avg_premium"))
)

vs_group = (
    filtered.alias("s")
    .join(product_stats.alias("g"), F.col("g.product") == F.col("s.product"), "inner")
    .filter(F.col("s.premium") > F.col("g.avg_premium"))
    .select(
        "s.*",
        "g.avg_premium",
        F.round(F.col("s.premium") - F.col("g.avg_premium"), 2).alias("premium_minus_avg"),
    )
)

with_details_holder = (
    vs_group.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.avg_premium",
        "p.holder_name",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.region").alias("holder_region"),
    )
)

result = (
    with_details_holder
    .filter(F.col("avg_premium") < 4280.0)
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "avg_premium",
        "holder_name",
        "holder_risk_score",
        F.abs(F.col("premium") - 3521.8).alias("premium_distance"),
    )
    .orderBy(F.col("avg_premium").desc(), F.col("id").desc())
)
