from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at_year_full = Window.orderBy("read_at_year", "id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_read_at_month_by_rea = Window.partitionBy("site_id", F.date_trunc("month", "read_at")).orderBy(F.col("reading_read_at").asc_nulls_last(), "id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_read_at_year = Window.orderBy("read_at_year", "id", "reading_id")
w_all_by_read_at_year_prior_3 = Window.orderBy("read_at_year", "id", "reading_id").rowsBetween(-3, -1)
w_reading_id_read_at_year = Window.partitionBy("reading_id", "read_at_year")
w_all_by_voltage = Window.orderBy(F.col("voltage").desc(), "id", "reading_id")
w_first_known_reading_read_at = Window.partitionBy(F.month("first_known_reading_read_at"))
w_read_at_year_by_read_at_full = Window.partitionBy("read_at_year").orderBy("read_at", "id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_year_by_read_at = Window.partitionBy("read_at_year").orderBy("read_at", "id", "reading_id")
w_all_by_voltage_ties = Window.orderBy("voltage")

prepared = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.year(F.col("read_at")).alias("read_at_year"),
    )
)

detail_rows_readings = (
    prepared.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.read_at_year",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
    )
)

windowed = (
    detail_rows_readings
    .select(
        "*",
        F.last("read_at").over(w_all_by_read_at_year_full).alias("last_read_at"),
        F.first("reading_read_at", ignorenulls=True).over(w_site_id_read_at_month_by_rea).alias("first_known_reading_read_at"),
        F.round(F.percent_rank().over(w_all_by_read_at_year), 3).alias("read_at_year_percent_rank"),
        F.round(F.avg("read_at_year").over(w_all_by_read_at_year_prior_3), 2).alias("read_at_year_avg_3_prior"),
        F.round(F.col("read_at_year") / F.nullif(F.sum("read_at_year").over(w_reading_id_read_at_year), F.lit(0)), 4).alias("read_at_year_share_of_reading_id_read_at_year"),
        F.round(F.cume_dist().over(w_all_by_voltage), 3).alias("voltage_cume_dist"),
    )
)

windowed_2 = (
    windowed
    .select(
        "id",
        "site_id",
        "voltage",
        "reading_id",
        "reading_read_at",
        "reading_quality",
        "last_read_at",
        "first_known_reading_read_at",
        "read_at_year_avg_3_prior",
        "read_at_year_share_of_reading_id_read_at_year",
        "voltage_cume_dist",
        F.size(F.collect_set("reading_quality").over(w_first_known_reading_read_at)).alias("distinct_reading_quality_in_first_known_reading_"),
        F.last("read_at_year_avg_3_prior").over(w_read_at_year_by_read_at_full).alias("last_read_at_year_avg_3_prior"),
        F.lag("voltage").over(w_read_at_year_by_read_at).alias("prev_voltage"),
        F.count(F.lit(1)).over(w_first_known_reading_read_at).alias("rows_in_first_known_reading_read_at_month"),
        F.rank().over(w_all_by_voltage_ties).alias("voltage_rank"),
        F.round(F.col("voltage_cume_dist") / F.nullif(F.sum("voltage_cume_dist").over(w_first_known_reading_read_at), F.lit(0)), 4).alias("voltage_cume_dist_share_of_first_known_reading_r"),
    )
)

result = (
    windowed_2
    .groupBy("voltage_rank", "reading_id")
    .pivot("reading_quality", ["OK", "EST", "BAD"])
    .agg(F.min("read_at_year_avg_3_prior"))
    .orderBy("voltage_rank", "reading_id")
)
