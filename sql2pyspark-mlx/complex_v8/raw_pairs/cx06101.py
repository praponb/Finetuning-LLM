from pyspark.sql import functions as F

scoped = (
    df_warehouses
    .filter(F.col("opened_on") < F.lit("2025-02-14").cast("date"))
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on",
        F.coalesce(F.col("city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("opened_on")).alias("days_since_opened_on"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("opened_on")).alias("days_since_opened_on_2"),
    )
)

screened = (
    scoped.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.moved_at") < F.lit("2025-03-02 10:00:00").cast("timestamp")), "left_semi")
)

city_or_unknown_stats = (
    screened
    .groupBy("city_or_unknown")
    .agg(F.percentile_approx("days_since_opened_on_2", 0.5).alias("median_days_since_opened_on_2"))
)

outliers = (
    screened.alias("s")
    .join(city_or_unknown_stats.alias("g"), F.col("g.city_or_unknown") == F.col("s.city_or_unknown"), "inner")
    .filter(F.col("s.days_since_opened_on_2") >= F.col("g.median_days_since_opened_on_2"))
    .select(
        "s.*",
        "g.median_days_since_opened_on_2",
        F.round(F.col("s.days_since_opened_on_2") - F.col("g.median_days_since_opened_on_2"), 2).alias("days_since_opened_on_2_minus_median"),
    )
)

stats = (
    outliers
    .groupBy(
        "city_or_unknown",
        F.date_trunc("month", "opened_on").alias("opened_on_month"),
    )
    .agg(
        F.min("days_since_opened_on_2_minus_median").alias("min_days_since_opened_on_2_minus_median"),
    )
)

result = (
    stats
    .select(
        "city_or_unknown",
        "opened_on_month",
        "min_days_since_opened_on_2_minus_median",
        F.coalesce(F.col("city_or_unknown"), F.lit("UNKNOWN")).alias("city_or_unknown_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("city_or_unknown")), " ", "_")).alias("city_or_unknown_slug"),
    )
    .orderBy(
        F.col("min_days_since_opened_on_2_minus_median").desc(),
        F.col("opened_on_month").desc_nulls_first(),
    )
)
