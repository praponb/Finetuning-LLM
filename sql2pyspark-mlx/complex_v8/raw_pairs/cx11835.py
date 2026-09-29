from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("zone").orderBy(F.col("rate").desc(), F.col("id").desc())

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

unique_rows = (
    joined_ranges
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

zone_stats = (
    unique_rows
    .groupBy("zone")
    .agg(F.avg("kwh").alias("avg_kwh"))
)

above_average = (
    unique_rows.alias("s")
    .join(zone_stats.alias("g"), F.col("g.zone") == F.col("s.zone"), "inner")
    .filter(F.col("s.kwh") > F.col("g.avg_kwh"))
    .select(
        "s.*",
        "g.avg_kwh",
        F.round(F.col("s.kwh") - F.col("g.avg_kwh"), 2).alias("kwh_minus_avg"),
    )
)

zone_stats_2 = (
    above_average
    .groupBy("zone")
    .agg(F.avg("rate").alias("avg_rate"))
)

outliers = (
    above_average.alias("s")
    .join(zone_stats_2.alias("g"), F.col("g.zone") == F.col("s.zone"), "inner")
    .filter(F.col("s.rate") > F.col("g.avg_rate"))
    .select(
        "s.*",
        "g.avg_rate",
        F.round(F.col("s.rate") - F.col("g.avg_rate"), 2).alias("rate_minus_avg"),
    )
)

result = (
    outliers
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "rate",
        "energy_cost",
        "avg_kwh",
        "kwh_minus_avg",
    )
    .orderBy(F.col("read_at").desc_nulls_first())
)
