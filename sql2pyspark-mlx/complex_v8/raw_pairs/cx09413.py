from pyspark.sql import functions as F

scoped = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
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

labeled = (
    scoped
    .select(
        "*",
        F.dayofweek(F.col("end_date")).alias("end_date_dayofweek"),
        F.concat(F.col("status"), F.lit("#"), F.col("holder_id").cast("string")).alias("status_ref"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

combined_details = (
    labeled.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select("s.*", F.col("p.segment").alias("holder_segment"))
)

result = (
    combined_details
    .select(
        "id",
        F.col("premium").alias("m_premium"),
        F.col("premium_filled").alias("premium_filled_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_premium", "premium_filled_v"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "metric_name")
)
