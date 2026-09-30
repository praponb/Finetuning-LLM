from pyspark.sql import functions as F

base_warehouses = (
    df_warehouses
    .filter(F.col("capacity") > 2614)
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        F.add_months(F.col("opened_on"), 3).alias("opened_on_plus_3m"),
    )
)

annotated = (
    base_warehouses
    .select(
        "*",
        F.when(F.col("city").isin("Singapore", "Kuala Lumpur"), "group_a").when(F.col("city") == "Bangkok", "group_b").otherwise("other").alias("city_group"),
        F.concat_ws("|", F.col("city"), F.col("wh_code")).alias("city_wh_code_label"),
        F.concat(F.col("city"), F.lit("#"), F.col("capacity").cast("string")).alias("city_ref"),
    )
)

annotated_2 = (
    annotated
    .filter(F.col("city_ref").isNotNull())
    .select(
        "*",
        F.when(F.col("capacity") >= 11506, "large").when(F.col("capacity") >= 4912, "medium").otherwise("small").alias("capacity_tier"),
        F.upper(F.trim(F.col("city"))).alias("city_clean"),
        F.date_trunc("year", F.col("opened_on_plus_3m")).alias("opened_on_plus_3m_year"),
    )
)

city_group_stats = (
    annotated_2
    .groupBy("city_group")
    .agg(F.avg("capacity").alias("avg_capacity"))
)

vs_group = (
    annotated_2.alias("s")
    .join(city_group_stats.alias("g"), F.col("g.city_group") == F.col("s.city_group"), "inner")
    .filter(F.col("s.capacity") > F.col("g.avg_capacity"))
    .select(
        "s.*",
        "g.avg_capacity",
        F.round(F.col("s.capacity") - F.col("g.avg_capacity"), 2).alias("capacity_minus_avg"),
    )
)

rollup_base = (
    vs_group
    .groupBy(
        F.date_trunc("month", "opened_on_plus_3m").alias("opened_on_plus_3m_month"),
        "city",
    )
    .agg(
        F.round(F.sum("capacity") / F.nullif(F.sum("capacity_minus_avg"), F.lit(0)), 3).alias("capacity_to_capacity_minus_avg_ratio"),
        F.round(F.sum("avg_capacity") / F.nullif(F.sum("capacity_minus_avg"), F.lit(0)), 3).alias("avg_capacity_to_capacity_minus_avg_ratio"),
        F.sum("avg_capacity").alias("total_avg_capacity"),
        F.min("avg_capacity").alias("min_avg_capacity"),
        F.max("capacity_minus_avg").alias("max_capacity_minus_avg"),
    )
    .filter(F.col("max_capacity_minus_avg") > -8000.0)
)

tagged = (
    rollup_base
    .select(
        "opened_on_plus_3m_month",
        "city",
        "capacity_to_capacity_minus_avg_ratio",
        "total_avg_capacity",
        "min_avg_capacity",
        "max_capacity_minus_avg",
        F.least(F.col("min_avg_capacity"), F.col("max_capacity_minus_avg")).alias("least_min_avg_capacity_max_capacity_minus_avg"),
    )
)

with_activity = (
    tagged
    .crossJoin(tagged.agg(F.max("max_capacity_minus_avg").alias("overall_max_max_capacity_minus_avg")))
    .filter(F.col("max_capacity_minus_avg") < F.col("overall_max_max_capacity_minus_avg"))
    .drop("overall_max_max_capacity_minus_avg")
)

result = (
    with_activity
    .filter(F.col("city").isin("Bangkok", "Kuala Lumpur"))
    .select(
        "opened_on_plus_3m_month",
        "city",
        "total_avg_capacity",
        "min_avg_capacity",
        "max_capacity_minus_avg",
    )
    .orderBy(F.col("city").desc(), "min_avg_capacity")
)
