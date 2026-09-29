from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_quality_len = Window.partitionBy("quality", "quality_len")
w_read_at_month_read_at_year_y = Window.partitionBy(F.month("read_at"), F.year("read_at_year")).orderBy("quality_len", "id")

filtered = (
    df_meter_readings
    .filter((F.col("site_id") >= 9) & (F.col("quality").isin("OK", "EST")))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

enriched = (
    filtered
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
        F.date_trunc("year", F.col("read_at")).alias("read_at_year"),
    )
)

enriched_2 = (
    enriched
    .filter(F.col("quality") != "EST")
    .select(
        "id",
        "read_at",
        "quality",
        "voltage_bucket",
        "quality_ref",
        "read_at_year",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.concat(F.col("quality_ref"), F.lit("#"), F.col("voltage_bucket").cast("string")).alias("quality_ref_ref"),
    )
)

windowed = (
    enriched_2
    .select(
        "id",
        "read_at",
        "quality",
        "voltage_bucket",
        "quality_ref",
        "read_at_year",
        "quality_len",
        "quality_ref_ref",
        F.round(F.avg("voltage_bucket").over(w_quality_quality_len), 2).alias("quality_quality_len_avg_voltage_bucket"),
        F.ntile(3).over(w_read_at_month_read_at_year_y).alias("quality_len_tercile"),
    )
)

result = (
    windowed
    .select(
        "id",
        "read_at",
        "quality",
        "voltage_bucket",
        "quality_ref",
        "quality_len",
        "quality_ref_ref",
        "quality_quality_len_avg_voltage_bucket",
        "quality_len_tercile",
        F.round(F.col("quality_len_tercile") / F.nullif(F.col("voltage_bucket"), F.lit(0)), 3).alias("quality_len_tercile_per_voltage_bucket"),
        (F.col("read_at") + F.expr("INTERVAL 7 DAYS")).alias("read_at_plus_7d"),
    )
    .orderBy(F.col("id").desc(), F.col("quality_len_tercile").desc())
)
