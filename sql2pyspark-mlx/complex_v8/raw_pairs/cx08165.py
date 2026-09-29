from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("site_id").orderBy(F.col("days_since_read_at").desc(), F.col("id").desc())

recent = (
    df_meter_readings
    .filter(F.col("voltage") <= 234.5)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.abs(F.col("voltage") - 222.5).alias("voltage_distance"),
    )
)

filtered = (
    recent.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.hour(F.col("m.read_at")).between(6, 10)) & (~F.col("m.quality").isin("OK", "BAD")), "left_semi")
)

screened = (
    filtered.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on") >= F.lit("2024-09-15").cast("date")), "left_anti")
)

joined = (
    screened.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality_prefix",
        "s.days_since_read_at",
        "s.voltage_distance",
        "si.site_name",
    )
)

latest = (
    joined
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_details_site = (
    latest.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.zone") == "Z4"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality_prefix",
        "s.voltage_distance",
        "s.site_name",
        F.col("si.zone").alias("site_zone"),
        F.col("si.site_name").alias("site_site_name"),
    )
)

result = (
    with_details_site
    .select("id", "kwh", "voltage_distance")
    .unpivot(
        ids=["id"],
        values=["kwh", "voltage_distance"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .orderBy("id", "measure")
)
