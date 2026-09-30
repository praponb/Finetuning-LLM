from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality_by_voltage_b = Window.partitionBy("site_id", "quality").orderBy(F.col("voltage_bucket").desc(), "id")
w_quality_len = Window.partitionBy("quality_len")
w_site_id_quality_by_voltage_b_2 = Window.partitionBy("site_id", "quality").orderBy(F.col("voltage_bucket").desc(), "id").rowsBetween(-6, Window.currentRow)

cleaned_readings = (
    df_meter_readings
    .filter(F.to_date(F.col("read_at")) < F.lit("2025-03-08").cast("date"))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

tagged = (
    cleaned_readings
    .filter(F.col("quality") != "EST")
    .select(
        "*",
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
    )
)

with_windows = (
    tagged
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality_len",
        "voltage_per_kwh",
        "voltage_bucket",
        F.lag("kwh", 1, 0).over(w_site_id_quality_by_voltage_b).alias("prev_kwh"),
        F.min("voltage_bucket").over(w_quality_len).alias("quality_len_min_voltage_bucket"),
        F.sum("voltage_bucket").over(w_site_id_quality_by_voltage_b_2).alias("voltage_bucket_sum_7"),
        (F.col("voltage_per_kwh") - F.lag("voltage_per_kwh").over(w_site_id_quality_by_voltage_b)).alias("voltage_per_kwh_change"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "site_id",
        "kwh",
        "quality_len",
        "voltage_per_kwh",
        "voltage_bucket",
        "prev_kwh",
        "quality_len_min_voltage_bucket",
        "voltage_bucket_sum_7",
    )
    .orderBy("voltage_bucket_sum_7", F.col("site_id").desc_nulls_first())
)
