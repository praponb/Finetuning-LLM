from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reading_count_by_reading_cou = Window.partitionBy("reading_count").orderBy(F.col("reading_count_quality_not_in_ok_est").desc(), "reading_count").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reading_count = Window.partitionBy("reading_count")
w_reading_count_by_total_volta = Window.partitionBy("reading_count").orderBy("total_voltage", "reading_count")

valid_readings = df_meter_readings.filter(F.col("site_id") >= 8)

qualified = (
    valid_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.hour(F.col("m.read_at")).between(8, 11)) & (F.col("m.kwh").between(37.0, 109.0)), "left_semi")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.count("*").alias("reading_count"))
)

profiled = (
    qualified.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

totals = (
    profiled
    .filter(F.col("reading_count") > 3)
    .groupBy("reading_count")
    .agg(
        F.round(F.avg("voltage"), 2).alias("avg_voltage"),
        F.sum("kwh").alias("total_kwh"),
        F.sum(F.when(~F.col("quality").isin("OK", "EST"), F.col("reading_count")).otherwise(0)).alias("reading_count_quality_not_in_ok_est"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.round(F.sum("voltage"), 2).alias("total_voltage"),
        F.sum("reading_count").alias("total_reading_count"),
        F.max("voltage").alias("max_voltage_check"),
    )
    .filter(F.col("max_voltage_check") > 220.2)
    .drop("max_voltage_check")
)

derived = (
    totals
    .filter(F.col("avg_voltage") <= 239.8)
    .select(
        "reading_count",
        "total_kwh",
        "reading_count_quality_not_in_ok_est",
        "quality_list",
        "total_voltage",
        "total_reading_count",
        F.abs(F.col("total_voltage") - 1092.0).alias("total_voltage_distance"),
    )
)

windowed = (
    derived
    .select(
        "reading_count",
        "total_kwh",
        "reading_count_quality_not_in_ok_est",
        "quality_list",
        "total_voltage",
        "total_reading_count",
        "total_voltage_distance",
        F.first("reading_count").over(w_reading_count_by_reading_cou).alias("first_reading_count"),
        F.round(F.col("reading_count") - F.avg("reading_count").over(w_reading_count), 2).alias("reading_count_vs_reading_count_avg"),
        F.row_number().over(w_reading_count_by_total_volta).alias("total_voltage_seq"),
        F.lead("total_voltage_distance").over(w_reading_count_by_total_volta).alias("next_total_voltage_distance"),
    )
)

enriched = (
    windowed
    .filter(F.col("total_voltage") <= 1092.0)
    .withColumn("total_kwh_tier", F.when(F.col("total_kwh") >= 540.0, "A").when(F.col("total_kwh") >= 216.0, "B").otherwise("C"))
)

result = (
    enriched
    .filter(F.col("reading_count_vs_reading_count_avg") > -6.4)
    .select(
        "reading_count",
        "total_kwh",
        "reading_count_quality_not_in_ok_est",
        "quality_list",
        "total_voltage",
        "total_reading_count",
        "total_voltage_distance",
        "first_reading_count",
        "total_voltage_seq",
        "next_total_voltage_distance",
        "total_kwh_tier",
    )
    .orderBy(
        F.col("total_voltage").desc(),
        F.col("total_voltage_distance").desc(),
        "reading_count",
    )
    .limit(10)
)
