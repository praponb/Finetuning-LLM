from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("site_id").orderBy(F.col("kwh_per_voltage").desc(), F.col("id").desc())

base_readings = (
    df_meter_readings
    .filter((F.col("voltage") < 227.6) & (F.to_date(F.col("read_at")) < F.lit("2025-03-08").cast("date")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.col("voltage") - F.col("kwh")).alias("voltage_minus_kwh"),
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
    )
)

enriched = (
    base_readings
    .filter(F.col("kwh") > 35.9)
    .select(
        "*",
        F.abs(F.col("voltage") - 221.4).alias("voltage_distance"),
        F.when(F.col("quality").isin("BAD", "OK"), "group_a").when(F.col("quality") == "EST", "group_b").otherwise("other").alias("quality_group"),
    )
)

with_details_site = (
    enriched.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.installed_kw") <= 284.0)
    .select(
        "s.*",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

unique_rows = (
    with_details_site
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.min("read_at").alias("first_reading_read_at"))
)

with_counts = (
    unique_rows.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.first_reading_read_at")
)

result = (
    with_counts
    .filter(~F.col("quality").isin("OK", "EST"))
    .select(
        "id",
        "site_id",
        "quality",
        "voltage_minus_kwh",
        "kwh_per_voltage",
        "site_installed_kw",
        "site_commissioned_on",
        "first_reading_read_at",
        F.least(F.col("voltage_distance"), F.col("voltage")).alias("least_voltage_distance_voltage"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("first_reading_read_at")).alias("days_since_first_reading_read_at"),
    )
    .orderBy(F.col("site_id").desc(), F.col("site_commissioned_on").desc())
)
