from pyspark.sql import functions as F

valid_warehouses = (
    df_warehouses
    .select(
        "id",
        "city",
        "capacity",
        "opened_on",
        F.abs(F.col("capacity") - 4912).alias("capacity_distance"),
        F.last_day(F.col("opened_on")).alias("opened_on_month_end"),
    )
)

tagged = (
    valid_warehouses
    .filter(~F.col("city").isin("Singapore", "Bangkok"))
    .select(
        "id",
        "city",
        "capacity",
        "opened_on",
        "capacity_distance",
        "opened_on_month_end",
        F.add_months(F.col("opened_on"), 1).alias("opened_on_plus_1m"),
        F.coalesce(F.col("city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
    )
)

city_or_unknown_stats = (
    tagged
    .groupBy("city_or_unknown")
    .agg(F.avg("capacity").alias("avg_capacity"))
)

compared = (
    tagged.alias("s")
    .join(city_or_unknown_stats.alias("g"), F.col("g.city_or_unknown") == F.col("s.city_or_unknown"), "inner")
    .filter(F.col("s.capacity") > F.col("g.avg_capacity"))
    .select(
        "s.*",
        "g.avg_capacity",
        F.round(F.col("s.capacity") - F.col("g.avg_capacity"), 2).alias("capacity_minus_avg"),
    )
)

result = (
    compared
    .groupingSets(
        [["city", "city_or_unknown"], ["city"], ["city_or_unknown"], []],
        "city", "city_or_unknown",
    )
    .agg(
        F.grouping("city").alias("is_total_city"),
        F.array_sort(F.collect_set("city")).alias("city_list"),
        F.round(F.sum("capacity_minus_avg"), 2).alias("total_capacity_minus_avg"),
    )
    .orderBy(
        F.col("city").asc_nulls_last(),
        F.col("city_or_unknown").asc_nulls_last(),
    )
)
