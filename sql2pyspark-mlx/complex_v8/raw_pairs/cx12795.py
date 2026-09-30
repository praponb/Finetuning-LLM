from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_site_ids_by_distinc = Window.partitionBy("distinct_site_ids").orderBy(F.col("distinct_site_ids_minus_total_voltage").desc())
w_distinct_site_ids_by_distinc_2 = Window.partitionBy("distinct_site_ids").orderBy(F.col("distinct_site_ids_minus_total_voltage").desc(), "read_at_day")
w_distinct_site_ids = Window.partitionBy("distinct_site_ids")
w_distinct_site_ids_by_distinc_3 = Window.partitionBy("distinct_site_ids").orderBy(F.col("distinct_site_ids_minus_total_voltage").desc(), "read_at_day").rowsBetween(-2, Window.currentRow)
w_all_by_total_voltage_full = Window.orderBy(F.col("total_voltage").desc(), "read_at_day").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_readings = (
    df_meter_readings
    .filter((F.col("voltage") < 232.4) & (F.hour(F.col("read_at")).between(9, 15)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality").isin("OK", "EST"), 1)).alias("reading_quality_count"),
        F.sum("kwh").alias("total_reading_kwh"),
        F.max("voltage").alias("max_reading_voltage"),
    )
)

summarized_rows = (
    scoped_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        "m.total_reading_kwh",
        "m.max_reading_voltage",
    )
)

stats = (
    summarized_rows
    .filter(F.col("quality_prefix").isNotNull())
    .groupBy(F.to_date("read_at").alias("read_at_day"))
    .agg(
        F.sum("voltage").alias("total_voltage"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
        F.min("read_at").alias("first_read_at"),
        F.round(F.sum("reading_quality_count") / F.nullif(F.sum("kwh"), F.lit(0)), 3).alias("reading_quality_count_to_kwh_ratio"),
    )
)

qualified = (
    stats
    .crossJoin(stats.agg(F.avg("distinct_site_ids").alias("overall_avg_distinct_site_ids")))
    .filter(F.col("distinct_site_ids") > F.col("overall_avg_distinct_site_ids"))
    .drop("overall_avg_distinct_site_ids")
)

without_matches = (
    qualified
    .crossJoin(qualified.agg(F.avg("distinct_site_ids").alias("overall_avg_distinct_site_ids")))
    .filter(F.col("distinct_site_ids") > F.col("overall_avg_distinct_site_ids"))
    .drop("overall_avg_distinct_site_ids")
)

annotated = (
    without_matches
    .select(
        "read_at_day",
        "total_voltage",
        "distinct_site_ids",
        "reading_quality_count_to_kwh_ratio",
        F.round(F.col("total_voltage") / F.nullif(F.col("distinct_site_ids"), F.lit(0)), 3).alias("total_voltage_per_distinct_site_ids"),
        (F.col("distinct_site_ids") - F.col("total_voltage")).alias("distinct_site_ids_minus_total_voltage"),
    )
)

ranked = (
    annotated
    .select(
        "*",
        F.dense_rank().over(w_distinct_site_ids_by_distinc).alias("distinct_site_ids_minus_total_voltage_dense_rank"),
        F.row_number().over(w_distinct_site_ids_by_distinc_2).alias("rn"),
        F.count(F.lit(1)).over(w_distinct_site_ids).alias("rows_in_distinct_site_ids"),
        F.sum("distinct_site_ids").over(w_distinct_site_ids_by_distinc_3).alias("distinct_site_ids_sum_3"),
        F.dense_rank().over(w_distinct_site_ids_by_distinc_2).alias("distinct_site_ids_minus_total_voltage_dense_rank_2"),
        F.nth_value("distinct_site_ids", 2).over(w_all_by_total_voltage_full).alias("second_distinct_site_ids"),
    )
)

result = (
    ranked
    .select(
        "read_at_day",
        "distinct_site_ids",
        "total_voltage_per_distinct_site_ids",
        "distinct_site_ids_minus_total_voltage",
        "distinct_site_ids_minus_total_voltage_dense_rank",
        "rn",
        "rows_in_distinct_site_ids",
        "distinct_site_ids_minus_total_voltage_dense_rank_2",
    )
    .orderBy("total_voltage_per_distinct_site_ids", F.col("read_at_day").desc())
)
