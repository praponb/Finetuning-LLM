from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_or_unknown_kwh_tier = Window.partitionBy("quality_or_unknown", "kwh_tier").orderBy("reading_count", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_month_by_reading_cou = Window.partitionBy(F.month("read_at")).orderBy("reading_count", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_month_by_reading_cou_2 = Window.partitionBy(F.month("read_at")).orderBy("reading_count", "id").rowsBetween(-3, Window.currentRow)
w_reading_count_by_reading_cou = Window.partitionBy("reading_count").orderBy("reading_count")
w_first_known_reading_count_qu = Window.partitionBy("first_known_reading_count", "quality")

recent_readings = (
    df_meter_readings
    .filter(F.col("kwh") <= 110.5)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
        F.when(F.col("kwh") >= 92.5, "large").when(F.col("kwh") >= 37.0, "medium").otherwise("small").alias("kwh_tier"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.count("*").alias("reading_count"))
)

profiled = (
    recent_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

ranked = (
    profiled
    .select(
        "id",
        "read_at",
        "quality",
        "quality_ref",
        "kwh_tier",
        "quality_or_unknown",
        "reading_count",
        F.first("reading_count", ignorenulls=True).over(w_quality_or_unknown_kwh_tier).alias("first_known_reading_count"),
        F.nth_value("read_at", 2).over(w_read_at_month_by_reading_cou).alias("second_read_at"),
        F.round(F.avg("kwh").over(w_read_at_month_by_reading_cou_2), 2).alias("kwh_avg_4"),
    )
)

windowed = (
    ranked
    .select(
        "*",
        F.dense_rank().over(w_reading_count_by_reading_cou).alias("reading_count_dense_rank"),
        F.min("kwh_avg_4").over(w_first_known_reading_count_qu).alias("first_known_reading_count_quality_min_kwh_avg_4"),
        F.round(F.avg("kwh_avg_4").over(w_first_known_reading_count_qu), 2).alias("first_known_reading_count_quality_avg_kwh_avg_4"),
        F.max("first_known_reading_count").over(w_first_known_reading_count_qu).alias("first_known_reading_count_quality_max_first_know"),
        F.count(F.lit(1)).over(w_first_known_reading_count_qu).alias("rows_in_first_known_reading_count_quality"),
    )
)

result = (
    windowed
    .groupingSets(
        [["reading_count", "quality_or_unknown"], ["reading_count"], ["quality_or_unknown"]],
        "reading_count", "quality_or_unknown",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.sum("reading_count_dense_rank") / F.nullif(F.sum("first_known_reading_count_quality_avg_kwh_avg_4"), F.lit(0)), 3).alias("reading_count_dense_rank_to_first_known_reading_"),
    )
    .orderBy(
        F.col("reading_count").asc_nulls_last(),
        F.col("quality_or_unknown").asc_nulls_last(),
    )
)
