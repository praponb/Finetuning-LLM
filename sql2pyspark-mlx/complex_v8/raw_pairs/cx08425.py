from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("site_id", "zone").orderBy(F.col("read_at").desc(), F.col("id").desc())

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

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("read_at").alias("first_reading_read_at"),
        F.count(F.when(F.col("quality") != "BAD", 1)).alias("reading_quality_count"),
        F.count("*").alias("reading_count"),
    )
)

with_counts = (
    joined_ranges.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.first_reading_read_at",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.round(F.avg("voltage"), 2).alias("avg_reading_voltage"))
)

summarized_rows = (
    with_counts.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.avg_reading_voltage")
)

deduped = (
    summarized_rows
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "zone",
        "rate",
        "energy_cost",
        "reading_count",
        "avg_reading_voltage",
    )
    .orderBy(F.col("kwh").desc())
)
