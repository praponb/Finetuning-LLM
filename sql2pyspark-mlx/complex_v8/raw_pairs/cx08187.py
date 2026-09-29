from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality_by_read_at_d = Window.partitionBy("site_id", "quality").orderBy(F.col("read_at_date").asc_nulls_last(), "id")
w_quality_len = Window.partitionBy("quality_len")
w_site_id_by_quality_len_runni = Window.partitionBy("site_id").orderBy("quality_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_len_by_read_at_date = Window.partitionBy("quality_len").orderBy(F.col("read_at_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_meter_readings
    .filter((F.col("quality").isin("EST", "BAD")) & (F.col("kwh") <= 81.1))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.to_date(F.col("read_at")).alias("read_at_date"),
    )
)

with_details = (
    filtered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.quality_len",
        "s.read_at_date",
        F.col("si.installed_kw").alias("site_installed_kw"),
        "si.site_name",
    )
)

filtered_2 = (
    with_details.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage").between(220.2, 233.2)) & (F.col("m.quality").isin("OK", "BAD")), "left_semi")
)

sequenced = (
    filtered_2
    .select(
        "*",
        F.lag("quality_len").over(w_site_id_quality_by_read_at_d).alias("prev_quality_len"),
        F.round(F.col("quality_len") - F.avg("quality_len").over(w_quality_len), 2).alias("quality_len_vs_quality_len_avg"),
        F.sum("site_installed_kw").over(w_site_id_by_quality_len_runni).alias("running_site_installed_kw"),
        F.nth_value("read_at_date", 2).over(w_quality_len_by_read_at_date).alias("second_read_at_date"),
    )
)

enriched = (
    sequenced.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.quality_len",
        "s.site_name",
        "s.quality_len_vs_quality_len_avg",
        "s.running_site_installed_kw",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

result = (
    enriched
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "kwh",
        "site_name",
        "quality_len_vs_quality_len_avg",
        "running_site_installed_kw",
        "site_tariff",
        "site_zone",
        "site_commissioned_on",
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
        F.when(F.col("quality").isin("EST", "BAD"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
    )
    .orderBy(
        F.col("site_tariff").desc(),
        F.col("quality_len_vs_quality_len_avg").desc_nulls_first(),
    )
)
