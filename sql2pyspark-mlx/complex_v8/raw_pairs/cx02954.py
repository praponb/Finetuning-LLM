from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_avg_kwh_per_voltage_p = Window.orderBy(F.col("avg_kwh_per_voltage").desc(), "kwh_per_voltage_tier").rowsBetween(-6, -1)
w_kwh_per_voltage_tier = Window.partitionBy("kwh_per_voltage_tier")

base_readings = (
    df_meter_readings
    .filter(F.col("voltage") < 233.1)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

combined_details_site = (
    base_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.kwh_per_voltage",
        "s.quality_slug",
        F.col("si.zone").alias("site_zone"),
        "si.site_name",
    )
)

totals = (
    combined_details_site
    .groupBy(
        F.when(F.col("kwh_per_voltage") >= 8.0, "HIGH").when(F.col("kwh_per_voltage") >= 3.0, "MEDIUM").otherwise("LOW").alias("kwh_per_voltage_tier"),
    )
    .agg(
        F.round(F.avg("kwh_per_voltage"), 2).alias("avg_kwh_per_voltage"),
        F.array_sort(F.collect_set("site_zone")).alias("site_zone_list"),
        F.count(F.when(F.col("quality") == "BAD", 1)).alias("n_quality_bad"),
        F.max("kwh_per_voltage").alias("max_kwh_per_voltage"),
        F.sum("kwh").alias("sum_kwh_check"),
    )
    .filter(F.col("sum_kwh_check") > 24.5)
    .drop("sum_kwh_check")
)

windowed = (
    totals
    .withColumn("max_kwh_per_voltage_avg_6_prior", F.round(F.avg("max_kwh_per_voltage").over(w_all_by_avg_kwh_per_voltage_p), 2))
    .withColumn("kwh_per_voltage_tier_min_n_quality_bad", F.min("n_quality_bad").over(w_kwh_per_voltage_tier))
)

result = (
    windowed
    .filter(F.col("kwh_per_voltage_tier").isin("LOW", "HIGH"))
    .select(
        "kwh_per_voltage_tier",
        "avg_kwh_per_voltage",
        "n_quality_bad",
        "max_kwh_per_voltage",
        (F.col("n_quality_bad") - F.col("avg_kwh_per_voltage")).alias("n_quality_bad_minus_avg_kwh_per_voltage"),
    )
)
