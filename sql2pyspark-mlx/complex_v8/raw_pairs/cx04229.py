from pyspark.sql import functions as F

recent_readings = (
    df_meter_readings
    .filter(F.hour(F.col("read_at")).between(8, 14))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

tagged = (
    recent_readings
    .filter(F.hour(F.col("read_at")).between(7, 11))
    .select("*", F.round(F.col("kwh") * 0.9, 2).alias("kwh_adj"))
)

with_details = (
    tagged.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.kwh_adj",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone"),
    )
)

result = (
    with_details
    .groupBy("read_at", "site_id")
    .pivot("site_zone", ["Z4", "Z3", "Z2"])
    .agg(F.count("kwh").alias("count_v"), F.sum("kwh").alias("sum_v"))
    .orderBy("read_at", "site_id")
)
