from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_group = Window.partitionBy("zone_group")
w_dedup = Window.partitionBy("site_id").orderBy(F.col("kwh").desc(), F.col("id").desc())

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
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

labeled = (
    joined_ranges
    .filter(F.col("kwh") <= 113.0)
    .select(
        "*",
        F.when(F.col("zone").isin("Z4", "Z3"), "group_a").when(F.col("zone") == "Z2", "group_b").otherwise("other").alias("zone_group"),
        F.round(F.col("voltage") / F.nullif(F.col("rate"), F.lit(0)), 3).alias("voltage_per_rate"),
    )
)

with_windows = (
    labeled
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "zone",
        "rate",
        "energy_cost",
        "zone_group",
        "voltage_per_rate",
        F.size(F.collect_set("site_id").over(w_zone_group)).alias("distinct_site_id_in_zone_group"),
        F.min("rate").over(w_zone_group).alias("zone_group_min_rate"),
    )
)

unique_rows = (
    with_windows
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "rate",
        "energy_cost",
        "voltage_per_rate",
        "distinct_site_id_in_zone_group",
        "zone_group_min_rate",
        F.abs(F.col("voltage") - 233.2).alias("voltage_distance"),
        F.dayofweek(F.col("read_at")).alias("read_at_dayofweek"),
    )
    .orderBy(F.col("voltage").desc())
)
