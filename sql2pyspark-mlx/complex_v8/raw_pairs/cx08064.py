from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("quality", "site_id").orderBy(F.col("voltage").desc(), F.col("id").desc())
w_quality_reading_id_by_readin = Window.partitionBy("quality", "reading_id").orderBy("reading_kwh", "id", "reading_id").rowsBetween(-1, -1)
w_site_id_reading_id_by_kwh_pr = Window.partitionBy("site_id", "reading_id").orderBy("kwh", "id", "reading_id").rowsBetween(-2, -1)
w_site_id_reading_id_by_kwh = Window.partitionBy("site_id", "reading_id").orderBy("kwh", "id", "reading_id")
w_site_id_reading_id_by_kwh_fu = Window.partitionBy("site_id", "reading_id").orderBy("kwh", "id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_reading_id = Window.partitionBy("site_id", "reading_id")

cleaned_readings = (
    df_meter_readings
    .filter(F.col("voltage").between(220.2, 234.5))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

latest_per_key = (
    cleaned_readings
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

line_level_readings = (
    latest_per_key.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

annotated = (
    line_level_readings
    .select("*", F.date_trunc("month", F.col("read_at")).alias("read_at_month"))
)

with_trends = (
    annotated
    .select(
        "*",
        F.sum("kwh").over(w_quality_reading_id_by_readin).alias("kwh_sum_1_prior"),
        F.round(F.avg("voltage").over(w_site_id_reading_id_by_kwh_pr), 2).alias("voltage_avg_2_prior"),
        F.lag("reading_kwh").over(w_site_id_reading_id_by_kwh).alias("prev_reading_kwh"),
        F.nth_value("reading_kwh", 2).over(w_site_id_reading_id_by_kwh_fu).alias("second_reading_kwh"),
        F.round(F.col("reading_voltage") / F.nullif(F.sum("reading_voltage").over(w_site_id_reading_id), F.lit(0)), 4).alias("reading_voltage_share_of_site_id_reading_id"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "reading_id",
        "reading_voltage",
        "reading_kwh",
        "read_at_month",
        "kwh_sum_1_prior",
        "voltage_avg_2_prior",
        "prev_reading_kwh",
        "second_reading_kwh",
        "reading_voltage_share_of_site_id_reading_id",
    )
    .orderBy(F.col("read_at").asc(), "id", "reading_id")
    .limit(10)
)
