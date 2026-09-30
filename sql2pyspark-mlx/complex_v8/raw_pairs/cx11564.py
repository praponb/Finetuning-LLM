from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_year_by_read_at_year = Window.partitionBy("read_at_year").orderBy("read_at_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_year_by_read_at_year_2 = Window.partitionBy("read_at_year").orderBy("read_at_year", "id").rowsBetween(-2, -1)

cleaned_readings = (
    df_meter_readings
    .filter(F.col("quality") == "OK")
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.year(F.col("read_at")).alias("read_at_year"),
    )
)

ranked = (
    cleaned_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "read_at_year",
        F.nth_value("kwh", 2).over(w_read_at_year_by_read_at_year).alias("second_kwh"),
        F.sum("read_at_year").over(w_read_at_year_by_read_at_year_2).alias("read_at_year_sum_2_prior"),
    )
)

labeled = ranked.withColumn("read_at_quarter", F.quarter(F.col("read_at")))

site_id_stats = (
    labeled
    .groupBy("site_id")
    .agg(F.percentile_approx("read_at_quarter", 0.5).alias("median_read_at_quarter"))
)

compared = (
    labeled.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.read_at_quarter") >= F.col("g.median_read_at_quarter"))
    .select(
        "s.*",
        "g.median_read_at_quarter",
        F.round(F.col("s.read_at_quarter") - F.col("g.median_read_at_quarter"), 2).alias("read_at_quarter_minus_median"),
    )
)

result = (
    compared
    .rollup("read_at_quarter", "site_id")
    .agg(
        F.min("read_at").alias("first_read_at"),
        F.round(F.sum("second_kwh") / F.nullif(F.sum("read_at_year"), F.lit(0)), 3).alias("second_kwh_to_read_at_year_ratio"),
        F.max("voltage").alias("max_voltage"),
    )
    .orderBy(
        F.col("read_at_quarter").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
    )
)
