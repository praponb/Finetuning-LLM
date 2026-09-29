from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id = Window.partitionBy("quality", "site_id")

cleaned_readings = (
    df_meter_readings
    .filter((F.col("kwh") >= 34.6) | (F.col("voltage") <= 231.7))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.when(F.col("quality").isin("BAD", "EST"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
    )
)

windowed = (
    cleaned_readings
    .select(
        "*",
        F.sum("kwh_bucket").over(w_quality_site_id).alias("quality_site_id_sum_kwh_bucket"),
    )
)

annotated = (
    windowed
    .filter(~F.col("quality_group").isin("group_b", "other"))
    .select(
        "*",
        F.concat(F.col("quality_group"), F.lit("#"), F.col("id").cast("string")).alias("quality_group_ref"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.to_date(F.col("read_at")).alias("read_at_date"),
    )
)

qualified = (
    annotated.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

enriched = (
    qualified.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
    )
)

result = (
    enriched
    .filter(F.col("site_zone") != "Z4")
    .select(
        "id",
        "read_at",
        "quality",
        "quality_group",
        "kwh_bucket",
        "quality_site_id_sum_kwh_bucket",
        "quality_group_ref",
        "days_since_read_at",
        "read_at_date",
        "site_installed_kw",
        "site_tariff",
        "site_zone",
        F.date_trunc("month", F.col("read_at_date")).alias("read_at_date_month"),
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket_2"),
    )
    .orderBy(F.col("id").desc(), F.col("kwh_bucket_2").desc_nulls_first())
)
