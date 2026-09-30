from pyspark.sql import functions as F

in_window = (
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

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("read_at").alias("last_reading_read_at"),
        F.max("read_at").alias("last_reading_read_at_2"),
        F.max("read_at").alias("last_reading_read_at_3"),
    )
)

summarized_rows = (
    in_window.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.last_reading_read_at",
        "m.last_reading_read_at_2",
        "m.last_reading_read_at_3",
    )
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.sum("kwh").alias("total_reading_kwh"),
    )
)

with_counts = (
    summarized_rows.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.total_reading_kwh",
    )
)

result = (
    with_counts
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "energy_cost",
        "last_reading_read_at",
        "last_reading_read_at_2",
        "last_reading_read_at_3",
        "reading_count",
        F.hour(F.col("last_reading_read_at_2")).alias("last_reading_read_at_2_hour"),
    )
    .orderBy(
        F.col("last_reading_read_at_2").desc_nulls_first(),
        F.col("last_reading_read_at_2_hour").asc_nulls_last(),
    )
)
