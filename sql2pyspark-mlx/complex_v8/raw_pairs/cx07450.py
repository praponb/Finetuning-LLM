from pyspark.sql import functions as F

filtered = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.round(F.col("voltage") * 0.25, 2).alias("voltage_adj"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.date_trunc("quarter", F.col("read_at")).alias("read_at_quarter"),
    )
)

annotated = (
    filtered
    .filter(F.col("voltage") > 216.8)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "voltage_adj",
        "quality_or_unknown",
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
        F.when(F.col("quality_or_unknown").isin("UNKNOWN", "BAD"), "group_a").when(F.col("quality_or_unknown") == "EST", "group_b").otherwise("other").alias("quality_or_unknown_group"),
    )
)

result = (
    annotated
    .select(
        "id",
        F.col("kwh_filled").alias("m_kwh_filled"),
        F.col("voltage").alias("voltage_v"),
        F.col("voltage_filled").alias("voltage_filled_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_kwh_filled", "voltage_v", "voltage_filled_v"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
