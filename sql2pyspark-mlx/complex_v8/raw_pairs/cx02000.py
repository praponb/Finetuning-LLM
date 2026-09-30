from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("quality").orderBy("kwh", "id")
w_site_id_read_at_month = Window.partitionBy("site_id", F.month("read_at"))
w_all_by_voltage_minus_avg_run = Window.orderBy("voltage_minus_avg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_year_site_id_by_volt = Window.partitionBy(F.year("read_at"), "site_id").orderBy("voltage", "id").rowsBetween(-1, Window.currentRow)
w_read_at_year_site_id_by_volt_2 = Window.partitionBy(F.year("read_at"), "site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_readings = (
    df_meter_readings
    .filter((F.col("read_at") >= F.lit("2025-01-19 09:00:00").cast("timestamp")) & (F.col("quality").isin("EST", "BAD")))
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
    scoped_readings
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

site_id_stats = (
    latest_per_key
    .groupBy("site_id")
    .agg(F.avg("voltage").alias("avg_voltage"))
)

outliers = (
    latest_per_key.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.voltage") > F.col("g.avg_voltage"))
    .select(
        "s.*",
        "g.avg_voltage",
        F.round(F.col("s.voltage") - F.col("g.avg_voltage"), 2).alias("voltage_minus_avg"),
    )
)

sequenced = (
    outliers
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "avg_voltage",
        "voltage_minus_avg",
        F.max("avg_voltage").over(w_site_id_read_at_month).alias("site_id_read_at_month_max_avg_voltage"),
        F.sum("voltage_minus_avg").over(w_all_by_voltage_minus_avg_run).alias("running_voltage_minus_avg"),
        F.count(F.lit(1)).over(w_site_id_read_at_month).alias("rows_in_site_id_read_at_month"),
        F.round(F.col("avg_voltage") / F.nullif(F.sum("avg_voltage").over(w_site_id_read_at_month), F.lit(0)), 4).alias("avg_voltage_share_of_site_id_read_at_month"),
        F.round(F.avg("voltage_minus_avg").over(w_read_at_year_site_id_by_volt), 2).alias("voltage_minus_avg_avg_2"),
        F.last("read_at", ignorenulls=True).over(w_read_at_year_site_id_by_volt_2).alias("last_known_read_at"),
    )
)

with_details_site = (
    sequenced.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.voltage_minus_avg",
        "s.site_id_read_at_month_max_avg_voltage",
        "s.avg_voltage_share_of_site_id_read_at_month",
        "s.voltage_minus_avg_avg_2",
        "s.last_known_read_at",
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

with_parent = (
    with_details_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.site_id_read_at_month_max_avg_voltage",
        "s.avg_voltage_share_of_site_id_read_at_month",
        "s.voltage_minus_avg_avg_2",
        "s.last_known_read_at",
        "s.site_commissioned_on",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone_2"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "site_id",
        "site_id_read_at_month_max_avg_voltage",
        "avg_voltage_share_of_site_id_read_at_month",
        "voltage_minus_avg_avg_2",
        "last_known_read_at",
        "site_installed_kw",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.length(F.trim(F.col("quality"))).alias("quality_len_2"),
    )
    .orderBy(F.col("quality_len").desc(), F.col("last_known_read_at").desc())
)
