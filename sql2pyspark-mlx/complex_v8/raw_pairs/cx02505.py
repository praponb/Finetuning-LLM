from pyspark.sql import functions as F

scoped = (
    df_tariff_windows
    .filter(F.col("tariff") != "TOU")
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        (F.col("valid_from") + F.expr("INTERVAL 15 MINUTES")).alias("valid_from_plus_15m"),
        F.concat(F.col("tariff"), F.lit("#"), F.col("id").cast("string")).alias("tariff_ref"),
        F.to_date(F.col("valid_to")).alias("valid_to_date"),
    )
)

tagged = (
    scoped
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("valid_to")) - F.unix_timestamp(F.col("valid_from_plus_15m"))) / 60, 1).alias("minutes_valid_from_plus_15m_to_valid_to"),
        F.to_date(F.col("valid_from")).alias("valid_from_date"),
    )
)

annotated = tagged.withColumn("tariff_clean", F.initcap(F.trim(F.col("tariff"))))

tariff_stats = (
    annotated
    .groupBy("tariff")
    .agg(F.avg("rate").alias("avg_rate"))
)

vs_group = (
    annotated.alias("s")
    .join(tariff_stats.alias("g"), F.col("g.tariff") == F.col("s.tariff"), "inner")
    .filter(F.col("s.rate") > F.col("g.avg_rate"))
    .select(
        "s.*",
        "g.avg_rate",
        F.round(F.col("s.rate") - F.col("g.avg_rate"), 2).alias("rate_minus_avg"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "minutes_valid_from_plus_15m_to_valid_to",
        "rate",
        "avg_rate",
    )
    .unpivot(
        ids=["id"],
        values=["minutes_valid_from_plus_15m_to_valid_to", "rate", "avg_rate"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
