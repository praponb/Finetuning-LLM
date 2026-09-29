from pyspark.sql import functions as F

filtered = (
    df_tariff_windows
    .filter((F.hour(F.col("valid_to")).between(7, 12)) | (F.col("tariff") == "FLAT"))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
        F.quarter(F.col("valid_to")).alias("valid_to_quarter"),
        (F.col("valid_to") + F.expr("INTERVAL 7 DAYS")).alias("valid_to_plus_7d"),
    )
)

per_group = (
    filtered
    .groupBy("tariff")
    .agg(
        F.min("rate").alias("min_rate"),
        F.max("rate").alias("max_rate"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("rate").alias("total_rate"),
    )
)

result = (
    per_group
    .select(
        "tariff",
        F.col("max_rate").alias("m_max_rate"),
        F.col("min_rate").alias("min_rate_v"),
    )
    .unpivot(
        ids=["tariff"],
        values=["m_max_rate", "min_rate_v"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("tariff", "field")
)
