from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("zone", "site_id").orderBy(F.col("read_at").desc(), F.col("id").desc())

matched = (
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

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.round(F.avg("kwh"), 2).alias("avg_reading_kwh"),
        F.max("kwh").alias("max_reading_kwh"),
        F.count("*").alias("reading_count"),
    )
)

summarized_rows = (
    matched.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.avg_reading_kwh",
        "m.max_reading_kwh",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

with_details_site = (
    summarized_rows.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.zone",
        "s.avg_reading_kwh",
        "s.max_reading_kwh",
        "s.reading_count",
        F.col("si.zone").alias("site_zone"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

deduped = (
    with_details_site
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "zone",
        "avg_reading_kwh",
        "max_reading_kwh",
        "reading_count",
        "site_zone",
        "site_tariff",
        F.when(F.col("zone").isin("Z4", "Z2"), "group_a").when(F.col("zone") == "Z3", "group_b").otherwise("other").alias("zone_group"),
    )
    .orderBy("site_zone")
)
