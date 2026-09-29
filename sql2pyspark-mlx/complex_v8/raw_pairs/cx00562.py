from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id_by_kwh_movin = Window.partitionBy("quality", "site_id").orderBy("kwh", "id").rowsBetween(-1, Window.currentRow)
w_quality = Window.partitionBy("quality")
w_quality_site_id_by_kwh_movin_2 = Window.partitionBy("quality", "site_id").orderBy("kwh", "id").rowsBetween(-6, Window.currentRow)
w_site_id_quality_by_voltage_p = Window.partitionBy("site_id", "quality").orderBy(F.col("voltage").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_site_id_quality_by_voltage_r = Window.partitionBy("site_id", "quality").orderBy(F.col("voltage").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_latest = Window.partitionBy("quality", "site_id").orderBy("kwh_sum_2_prior", "id")
w_dedup = Window.partitionBy("site_id").orderBy("kwh_sum_2_prior", "id")

prepared = (
    df_meter_readings
    .filter((F.col("quality").isNotNull()) | (F.col("voltage") <= 231.6))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        (F.col("kwh") - F.col("voltage")).alias("kwh_minus_voltage"),
        (F.col("read_at") + F.expr("INTERVAL 2 HOURS")).alias("read_at_plus_2h"),
    )
)

filtered = (
    prepared
    .crossJoin(prepared.agg(F.avg("kwh").alias("overall_avg_kwh")))
    .filter(F.col("kwh") > F.col("overall_avg_kwh"))
    .drop("overall_avg_kwh")
)

ranked = (
    filtered
    .select(
        "*",
        F.round(F.avg("kwh").over(w_quality_site_id_by_kwh_movin), 2).alias("kwh_avg_2"),
        F.round(F.col("kwh_minus_voltage") - F.avg("kwh_minus_voltage").over(w_quality), 2).alias("kwh_minus_voltage_vs_quality_avg"),
        F.round(F.avg("kwh_minus_voltage").over(w_quality_site_id_by_kwh_movin_2), 2).alias("kwh_minus_voltage_avg_7"),
        F.sum("kwh").over(w_site_id_quality_by_voltage_p).alias("kwh_sum_2_prior"),
        F.last("kwh_minus_voltage", ignorenulls=True).over(w_site_id_quality_by_voltage_r).alias("last_known_kwh_minus_voltage"),
    )
)

latest = (
    ranked
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

unique_rows = (
    latest
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

detail_rows = (
    unique_rows.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.read_at_plus_2h",
        "s.kwh_avg_2",
        "s.kwh_minus_voltage_vs_quality_avg",
        "s.kwh_sum_2_prior",
        "s.last_known_kwh_minus_voltage",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.quality").alias("reading_quality"),
    )
)

combined_details_reading = (
    detail_rows.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.id") == F.col("s.reading_id")) & (~F.col("m.quality").isin("BAD", "EST")), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.kwh_avg_2",
        "s.kwh_minus_voltage_vs_quality_avg",
        "s.kwh_sum_2_prior",
        "s.last_known_kwh_minus_voltage",
        "s.reading_id",
        "s.reading_read_at",
        "s.reading_voltage",
        F.col("m.site_id").alias("reading_site_id"),
        F.col("m.read_at").alias("reading_read_at_2"),
        F.col("m.quality").alias("reading_quality_2"),
    )
)

result = (
    combined_details_reading
    .select(
        "id",
        "kwh",
        "voltage",
        "kwh_avg_2",
        "kwh_minus_voltage_vs_quality_avg",
        "kwh_sum_2_prior",
        "last_known_kwh_minus_voltage",
        "reading_read_at",
        "reading_voltage",
        "reading_site_id",
        "reading_read_at_2",
        "reading_quality_2",
        (F.col("reading_read_at_2") + F.expr("INTERVAL 7 DAYS")).alias("reading_read_at_2_plus_7d"),
    )
    .orderBy(
        F.col("last_known_kwh_minus_voltage").asc_nulls_last(),
        "reading_read_at",
    )
)
