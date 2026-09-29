from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("quality_or_unknown", "site_id").orderBy(F.col("read_at").desc(), F.col("id").desc())
w_n_quality_in_ok_bad = Window.partitionBy("n_quality_in_ok_bad")
w_read_at_month_to_date_by_n_q = Window.partitionBy(F.to_date("read_at_month")).orderBy(F.col("n_quality_in_ok_bad").desc(), "reading_id", "read_at_month", "reading_read_at_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_month_to_date_by_n_q_2 = Window.partitionBy(F.to_date("read_at_month")).orderBy(F.col("n_quality_in_ok_bad").desc(), "reading_id", "read_at_month", "reading_read_at_month").rowsBetween(-1, -1)

scoped_readings = (
    df_meter_readings
    .filter(F.col("kwh") >= 29.2)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

tagged = (
    scoped_readings
    .withColumn("quality_or_unknown_prefix", F.substring(F.trim(F.col("quality_or_unknown")), 1, 2))
)

latest_per_key = (
    tagged
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

detail_rows_readings = (
    latest_per_key.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.kwh_filled",
        "s.quality_or_unknown",
        "s.quality_or_unknown_prefix",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

totals = (
    detail_rows_readings
    .groupBy(
        "reading_id",
        F.date_trunc("month", "read_at").alias("read_at_month"),
        F.date_trunc("month", "reading_read_at").alias("reading_read_at_month"),
    )
    .agg(
        F.round(F.sum("reading_kwh"), 2).alias("total_reading_kwh"),
        F.count(F.when(F.col("quality").isin("OK", "BAD"), 1)).alias("n_quality_in_ok_bad"),
        F.max("voltage").alias("max_voltage_check"),
    )
    .filter(F.col("max_voltage_check") > 215.8)
    .drop("max_voltage_check")
)

with_trends = (
    totals
    .select(
        "reading_id",
        "read_at_month",
        "reading_read_at_month",
        "total_reading_kwh",
        "n_quality_in_ok_bad",
        F.round(F.col("total_reading_kwh") / F.nullif(F.sum("total_reading_kwh").over(w_n_quality_in_ok_bad), F.lit(0)), 4).alias("total_reading_kwh_share_of_n_quality_in_ok_bad"),
        F.round(F.avg("total_reading_kwh").over(w_n_quality_in_ok_bad), 2).alias("n_quality_in_ok_bad_avg_total_reading_kwh"),
        F.first("reading_read_at_month", ignorenulls=True).over(w_read_at_month_to_date_by_n_q).alias("first_known_reading_read_at_month"),
        F.round(F.avg("total_reading_kwh").over(w_read_at_month_to_date_by_n_q_2), 2).alias("total_reading_kwh_avg_1_prior"),
        F.count(F.lit(1)).over(w_n_quality_in_ok_bad).alias("rows_in_n_quality_in_ok_bad"),
    )
)

result = (
    with_trends
    .select(
        "reading_id",
        "read_at_month",
        "reading_read_at_month",
        "total_reading_kwh",
        "n_quality_in_ok_bad",
        "total_reading_kwh_share_of_n_quality_in_ok_bad",
        "n_quality_in_ok_bad_avg_total_reading_kwh",
        "first_known_reading_read_at_month",
        "total_reading_kwh_avg_1_prior",
    )
    .orderBy(F.col("reading_read_at_month").desc())
)
