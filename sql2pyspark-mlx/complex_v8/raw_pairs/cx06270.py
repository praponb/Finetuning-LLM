from pyspark.sql import functions as F

filtered = (
    df_sites
    .filter((F.year(F.col("commissioned_on")) == 2025) & (F.upper(F.trim(F.col("site_name"))).like("S%")))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.date_format(F.col("commissioned_on"), "yyyy-MM-dd").alias("commissioned_on_label"),
    )
)

totals = (
    filtered
    .groupBy("tariff", "zone")
    .agg(
        F.countDistinct("zone").alias("distinct_zones"),
        F.sum(F.when(F.col("zone") == "Z4", 1).otherwise(0)).alias("n_zone_z4"),
        F.round(F.avg("installed_kw"), 2).alias("avg_installed_kw"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.upper(F.trim(F.col("site_name"))).like("R%"), 1)).alias("n_upper_trim_site_name_like_r"),
        F.sum(F.when(F.col("zone") == "Z3", F.col("installed_kw"))).alias("installed_kw_zone_z3"),
    )
    .filter(F.col("cnt") >= 2)
)

qualified = (
    totals
    .crossJoin(totals.agg(F.max("cnt").alias("overall_max_cnt")))
    .filter(F.col("cnt") < F.col("overall_max_cnt"))
    .drop("overall_max_cnt")
)

result = (
    qualified
    .select(
        "tariff",
        "zone",
        "avg_installed_kw",
        "installed_kw_zone_z3",
    )
    .unpivot(
        ids=["tariff", "zone"],
        values=["avg_installed_kw", "installed_kw_zone_z3"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("tariff", "zone", "measure")
)
