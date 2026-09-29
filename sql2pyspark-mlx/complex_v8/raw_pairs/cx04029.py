from pyspark.sql import functions as F

joined_ranges = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "left",
    )
    .select(
        "r.id",
        "r.site_id",
        "r.read_at",
        "r.kwh",
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

rollup_base = (
    joined_ranges
    .filter(F.col("kwh") <= 81.1)
    .groupBy(F.to_date("read_at").alias("read_at_day"), "site_id", "zone")
    .agg(
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.min("energy_cost").alias("min_energy_cost"),
        F.count("*").alias("row_count"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
        F.avg("rate").alias("avg_rate_check"),
    )
    .filter(F.col("avg_rate_check") > 3.7)
    .drop("avg_rate_check")
)

tagged = (
    rollup_base
    .filter(F.col("min_energy_cost") < 675.0)
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("zone")), " ", "_")).alias("zone_slug"),
    )
)

result = (
    tagged
    .select(
        "read_at_day",
        "site_id",
        "zone",
        "min_energy_cost",
        "row_count",
        "distinct_site_ids",
        F.greatest(F.col("distinct_site_ids"), F.col("min_energy_cost")).alias("greatest_distinct_site_ids_min_energy_cost"),
        F.date_format(F.col("read_at_day"), "yyyy-MM-dd").alias("read_at_day_label"),
    )
    .orderBy(
        F.col("distinct_site_ids").desc_nulls_first(),
        F.col("read_at_day_label").asc_nulls_last(),
    )
)
