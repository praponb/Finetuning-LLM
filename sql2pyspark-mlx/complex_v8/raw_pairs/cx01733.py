from pyspark.sql import functions as F

valid = (
    df_sites
    .filter((F.col("commissioned_on").between(F.lit("2024-06-27").cast("date"), F.lit("2025-11-28").cast("date"))) & (F.col("tariff") == "EV"))
    .select(
        "id",
        "site_name",
        "tariff",
        "installed_kw",
        F.abs(F.col("installed_kw") - 283.9).alias("installed_kw_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("tariff")), " ", "_")).alias("tariff_slug"),
    )
)

summary = (
    valid
    .groupBy("tariff")
    .agg(
        F.round(F.sum("installed_kw_distance") / F.nullif(F.sum("installed_kw"), F.lit(0)), 3).alias("installed_kw_distance_to_installed_kw_ratio"),
        F.round(F.stddev("installed_kw_distance"), 2).alias("stddev_installed_kw_distance"),
        F.array_sort(F.collect_set("tariff")).alias("tariff_list"),
        F.round(F.sum("installed_kw"), 2).alias("total_installed_kw"),
        F.avg("installed_kw").alias("avg_installed_kw_check"),
    )
    .filter(F.col("avg_installed_kw_check") > 96.9)
    .drop("avg_installed_kw_check")
)

result = (
    summary
    .select(
        "tariff",
        "installed_kw_distance_to_installed_kw_ratio",
        "stddev_installed_kw_distance",
        "total_installed_kw",
    )
    .unpivot(
        ids=["tariff"],
        values=["installed_kw_distance_to_installed_kw_ratio", "stddev_installed_kw_distance", "total_installed_kw"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
