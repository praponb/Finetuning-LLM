from pyspark.sql import functions as F

valid = (
    df_sites
    .filter(F.col("zone").isin("Z3", "Z2"))
    .select(
        "id",
        "site_name",
        "tariff",
        "commissioned_on",
        F.length(F.trim(F.col("tariff"))).alias("tariff_len"),
    )
)

without_matches = (
    valid
    .crossJoin(valid.agg(F.avg("tariff_len").alias("overall_avg_tariff_len")))
    .filter(F.col("tariff_len") > F.col("overall_avg_tariff_len"))
    .drop("overall_avg_tariff_len")
)

qualified = (
    without_matches
    .crossJoin(without_matches.agg(F.max("tariff_len").alias("overall_max_tariff_len")))
    .filter(F.col("tariff_len") < F.col("overall_max_tariff_len"))
    .drop("overall_max_tariff_len")
)

labeled = (
    qualified
    .select(
        "*",
        F.coalesce(F.col("tariff_len"), F.lit(0)).alias("tariff_len_filled"),
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
    )
)

result = (
    labeled
    .select("id", "tariff_len", "tariff_len_filled")
    .unpivot(
        ids=["id"],
        values=["tariff_len", "tariff_len_filled"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "field")
)
