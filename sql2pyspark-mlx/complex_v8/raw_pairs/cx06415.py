from pyspark.sql import functions as F

valid_tariffs = (
    df_tariff_windows
    .filter((F.to_date(F.col("valid_to")) < F.lit("2025-02-18").cast("date")) & (F.col("rate") >= 3.0))
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        F.when(F.col("tariff").isin("TOU", "EV"), "group_a").when(F.col("tariff") == "FLAT", "group_b").otherwise("other").alias("tariff_group"),
    )
)

screened = (
    valid_tariffs
    .crossJoin(valid_tariffs.agg(F.max("rate").alias("overall_max_rate")))
    .filter(F.col("rate") < F.col("overall_max_rate"))
    .drop("overall_max_rate")
)

rollup_base = (
    screened
    .filter(F.col("rate") > 5.2)
    .groupBy(F.date_format("valid_from", "yyyy-MM").alias("valid_from_ym"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.countDistinct("tariff_group").alias("distinct_tariff_groups"),
        F.max("valid_to").alias("last_valid_to"),
        F.avg("rate").alias("avg_rate_check"),
    )
    .filter(F.col("avg_rate_check") > 3.7)
    .drop("avg_rate_check")
)

result = (
    rollup_base
    .select(
        "valid_from_ym",
        F.col("distinct_ids").alias("distinct_ids_v"),
        F.col("distinct_tariff_groups").alias("distinct_tariff_groups_v"),
    )
    .unpivot(
        ids=["valid_from_ym"],
        values=["distinct_ids_v", "distinct_tariff_groups_v"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
)
