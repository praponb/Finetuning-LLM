from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reading_count_by_kwh_per_vol = Window.partitionBy("reading_count").orderBy("kwh_per_voltage", "id").rowsBetween(-1, -1)
w_first_reading_read_at_year = Window.partitionBy(F.year("first_reading_read_at"))
w_all_by_first_reading_read_at = Window.orderBy(F.col("first_reading_read_at").desc())
w_quality = Window.partitionBy("quality")
w_reading_count_quality_by_rea = Window.partitionBy("reading_count", "quality").orderBy("read_at", "id").rowsBetween(-1, -1)

cleaned_readings = (
    df_meter_readings
    .filter((F.col("kwh") > 37.1) | (F.col("site_id") >= 8))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.count(F.when(~F.col("quality").isin("OK", "EST"), 1)).alias("reading_quality_count"),
        F.min("read_at").alias("first_reading_read_at"),
    )
)

with_counts = (
    cleaned_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        "m.first_reading_read_at",
    )
)

sequenced = (
    with_counts
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "voltage_per_kwh",
        "reading_count",
        "reading_quality_count",
        "first_reading_read_at",
        F.round(F.avg("voltage_per_kwh").over(w_reading_count_by_kwh_per_vol), 2).alias("voltage_per_kwh_avg_1_prior"),
        F.min("voltage").over(w_first_reading_read_at_year).alias("first_reading_read_at_year_min_voltage"),
        F.round(F.cume_dist().over(w_all_by_first_reading_read_at), 3).alias("first_reading_read_at_cume_dist"),
    )
)

ranked = (
    sequenced
    .withColumn("quality_max_voltage_per_kwh_avg_1_prior", F.max("voltage_per_kwh_avg_1_prior").over(w_quality))
    .withColumn("voltage_per_kwh_sum_1_prior", F.sum("voltage_per_kwh").over(w_reading_count_quality_by_rea))
)

quality_stats = (
    ranked
    .groupBy("quality")
    .agg(F.percentile_approx("kwh", 0.5).alias("median_kwh"))
)

above_average = (
    ranked.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.kwh") >= F.col("g.median_kwh"))
    .select(
        "s.*",
        "g.median_kwh",
        F.round(F.col("s.kwh") - F.col("g.median_kwh"), 2).alias("kwh_minus_median"),
    )
)

result = (
    above_average
    .select(
        "id",
        "read_at",
        "voltage",
        "reading_count",
        "first_reading_read_at",
        "voltage_per_kwh_avg_1_prior",
        "first_reading_read_at_year_min_voltage",
        "quality_max_voltage_per_kwh_avg_1_prior",
        "voltage_per_kwh_sum_1_prior",
        "median_kwh",
        "kwh_minus_median",
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
    )
)
