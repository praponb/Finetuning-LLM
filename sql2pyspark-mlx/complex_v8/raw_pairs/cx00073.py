from pyspark.sql import functions as F

joined_ranges = (
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
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

grouped = (
    joined_ranges
    .groupBy("zone", "quality")
    .agg(
        F.max("read_at").alias("last_read_at"),
        F.max("energy_cost").alias("max_energy_cost"),
        F.round(F.avg("rate"), 2).alias("avg_rate"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("energy_cost"), 2).alias("total_energy_cost"),
    )
)

result = (
    grouped
    .select(
        "zone",
        "quality",
        "last_read_at",
        "max_energy_cost",
        "avg_rate",
        "cnt",
        "distinct_ids",
        "total_energy_cost",
        (F.col("total_energy_cost") + F.col("avg_rate")).alias("total_energy_cost_plus_avg_rate"),
    )
    .orderBy(F.col("cnt").asc_nulls_last(), "max_energy_cost")
)
