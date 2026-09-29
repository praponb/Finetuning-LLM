from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("zone").orderBy("kwh", "id")

covered = (
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

with_activity = (
    covered.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality").isin("EST", "OK")) & (F.col("m.kwh") >= 52.5), "left_semi")
    .filter(F.col("kwh") <= 109.0)
)

latest = (
    with_activity
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

filtered = (
    latest.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality").isNotNull()) & (F.col("m.read_at") < F.lit("2025-02-18 12:00:00").cast("timestamp")), "left_anti")
)

result = (
    filtered
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "zone",
        "rate",
        "energy_cost",
        F.when(F.col("zone").isin("Z3", "Z4"), "group_a").when(F.col("zone") == "Z2", "group_b").otherwise("other").alias("zone_group"),
        F.coalesce(F.col("zone"), F.lit("UNKNOWN")).alias("zone_or_unknown"),
    )
)
