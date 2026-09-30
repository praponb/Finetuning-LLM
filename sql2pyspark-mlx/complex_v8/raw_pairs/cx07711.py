from pyspark.sql import functions as F

in_window = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "inner",
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

scored = (
    in_window
    .filter(F.col("site_id").isNotNull())
    .select(
        "*",
        F.coalesce(F.col("zone"), F.lit("UNKNOWN")).alias("zone_or_unknown"),
        F.greatest(F.col("voltage"), F.col("kwh")).alias("greatest_voltage_kwh"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality") == "BAD", 1)).alias("reading_quality_count"),
        F.count(F.when(F.col("quality").isin("OK", "EST"), 1)).alias("reading_quality_count_2"),
        F.count(F.when(F.col("quality").isin("OK", "BAD"), 1)).alias("reading_quality_count_3"),
    )
)

with_counts = (
    scored.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        F.coalesce(F.col("m.reading_quality_count_2"), F.lit(0)).alias("reading_quality_count_2"),
        F.coalesce(F.col("m.reading_quality_count_3"), F.lit(0)).alias("reading_quality_count_3"),
    )
)

derived = (
    with_counts
    .withColumn("reading_quality_count_2_plus_rate", F.col("reading_quality_count_2") + F.col("rate"))
)

result = (
    derived
    .rollup("reading_quality_count_2", "site_id")
    .agg(
        F.grouping("reading_quality_count_2").alias("is_total_reading_quality_count_2"),
        F.round(F.sum("voltage"), 2).alias("total_voltage"),
        F.sum(F.when(F.col("zone") == "Z3", F.col("energy_cost"))).alias("energy_cost_zone_z3"),
        F.sum(F.when(F.col("zone").isNotNull(), 1).otherwise(0)).alias("n_zone_is_not_null"),
    )
)
