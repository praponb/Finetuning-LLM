from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_year_by_voltage_qual = Window.partitionBy("read_at_year").orderBy("voltage_quality_bad_distance", "read_at_year")
w_read_at_year_by_voltage_qual_2 = Window.partitionBy("read_at_year").orderBy("voltage_quality_bad_distance", "read_at_year").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_meter_readings
    .filter(F.col("voltage") >= 222.3)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
        F.year(F.col("read_at")).alias("read_at_year"),
    )
)

summary = (
    cleaned
    .filter(F.col("quality_prefix").isNotNull())
    .groupBy("read_at_year")
    .agg(
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.round(F.sum("voltage") / F.nullif(F.sum("read_at_year"), F.lit(0)), 3).alias("voltage_to_read_at_year_ratio"),
        F.round(100 * F.count(F.when(F.col("quality") == "BAD", 1)) / F.count("*"), 1).alias("pct_quality_bad"),
        F.sum(F.when(F.col("quality").isin("BAD", "EST"), F.col("voltage"))).alias("voltage_quality_in_bad_est"),
        F.sum(F.when(F.col("quality") == "BAD", F.col("voltage")).otherwise(0)).alias("voltage_quality_bad"),
        F.max("voltage").alias("max_voltage_check"),
    )
    .filter(F.col("max_voltage_check") > 220.2)
    .drop("max_voltage_check")
)

labeled = (
    summary
    .filter(F.col("pct_quality_bad") <= 55.0)
    .select(
        "*",
        F.abs(F.col("voltage_quality_bad") - 367.5).alias("voltage_quality_bad_distance"),
        F.round(F.col("voltage_quality_bad") * 1.07, 2).alias("voltage_quality_bad_adj"),
    )
)

with_activity = (
    labeled
    .crossJoin(labeled.agg(F.avg("pct_quality_bad").alias("overall_avg_pct_quality_bad")))
    .filter(F.col("pct_quality_bad") > F.col("overall_avg_pct_quality_bad"))
    .drop("overall_avg_pct_quality_bad")
)

ranked = (
    with_activity
    .select(
        "*",
        F.lag("voltage_quality_in_bad_est").over(w_read_at_year_by_voltage_qual).alias("prev_voltage_quality_in_bad_est"),
        F.first("voltage_quality_bad_adj", ignorenulls=True).over(w_read_at_year_by_voltage_qual_2).alias("first_known_voltage_quality_bad_adj"),
    )
)

result = (
    ranked
    .select(
        "read_at_year",
        "quality_list",
        "pct_quality_bad",
        "voltage_quality_bad_distance",
        "prev_voltage_quality_in_bad_est",
        "first_known_voltage_quality_bad_adj",
    )
    .orderBy(
        F.col("first_known_voltage_quality_bad_adj").asc_nulls_last(),
        "read_at_year",
    )
    .limit(10)
)
