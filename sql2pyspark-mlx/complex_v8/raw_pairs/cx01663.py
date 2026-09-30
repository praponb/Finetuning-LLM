from pyspark.sql import functions as F

recent_policys = (
    df_policies
    .filter(F.col("holder_id") >= 10)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
        F.abs(F.col("premium") - 3521.8).alias("premium_distance"),
    )
)

with_parent = (
    recent_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        "p.holder_name",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

qualified = (
    with_parent.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.premium") <= 3544.1), "left_semi")
)

result = (
    qualified
    .cube("product", "holder_segment")
    .agg(
        F.round(F.sum("premium"), 2).alias("total_premium"),
        F.countDistinct("product").alias("distinct_products"),
    )
    .orderBy(
        F.col("product").asc_nulls_last(),
        F.col("holder_segment").asc_nulls_last(),
    )
)
