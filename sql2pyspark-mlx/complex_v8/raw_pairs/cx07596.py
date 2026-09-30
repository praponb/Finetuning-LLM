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
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

per_group = (
    joined_ranges
    .groupBy(
        "zone",
        "site_id",
        F.date_trunc("month", "read_at").alias("read_at_month"),
    )
    .agg(
        F.round(F.avg("rate"), 2).alias("avg_rate"),
        F.array_sort(F.collect_set("zone")).alias("zone_list"),
        F.countDistinct("zone").alias("distinct_zones"),
        F.min("read_at").alias("first_read_at"),
        F.count("*").alias("n_rows"),
        F.round(F.sum("energy_cost") / F.nullif(F.sum("rate"), F.lit(0)), 3).alias("energy_cost_to_rate_ratio"),
    )
)

labeled = (
    per_group
    .filter(F.col("energy_cost_to_rate_ratio") < 7.5)
    .select(
        "*",
        F.when(F.col("n_rows") >= 7, "large").when(F.col("n_rows") >= 4, "medium").otherwise("small").alias("n_rows_tier"),
    )
)

enriched = (
    labeled
    .select(
        "*",
        (F.col("read_at_month") + F.expr("INTERVAL 7 DAYS")).alias("read_at_month_plus_7d"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at_month")).alias("days_since_read_at_month"),
        F.least(F.col("n_rows"), F.col("avg_rate")).alias("least_n_rows_avg_rate"),
    )
)

result = (
    enriched
    .groupBy("n_rows", "read_at_month")
    .pivot("zone", ["Z2", "Z3"])
    .agg(F.min("avg_rate"))
)
