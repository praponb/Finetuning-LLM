from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_quality_site_tariff_by_volta = Window.partitionBy("quality", "site_tariff").orderBy("voltage")
w_quality_site_tariff_by_volta_2 = Window.partitionBy("quality", "site_tariff").orderBy("voltage", "id").rowsBetween(-3, Window.currentRow)
w_quality_site_tariff_by_volta_3 = Window.partitionBy("quality", "site_tariff").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_meter_readings
    .filter((F.hour(F.col("read_at")).between(9, 14)) | (F.col("voltage") <= 238.8))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "yyyy-MM").alias("read_at_label"),
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label_2"),
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
    )
)

combined_details = (
    recent.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.read_at_label_2",
        "si.site_name",
        F.col("si.tariff").alias("site_tariff"),
    )
)

windowed = (
    combined_details
    .select(
        "*",
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id), 2).alias("voltage_vs_site_id_avg"),
        F.rank().over(w_quality_site_tariff_by_volta).alias("voltage_rank"),
        F.sum("voltage").over(w_quality_site_tariff_by_volta_2).alias("voltage_sum_4"),
        F.last("read_at", ignorenulls=True).over(w_quality_site_tariff_by_volta_3).alias("last_known_read_at"),
    )
)

with_details = (
    windowed.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.read_at_label_2",
        "s.site_name",
        "s.site_tariff",
        "s.voltage_vs_site_id_avg",
        "s.voltage_rank",
        "s.voltage_sum_4",
        "s.last_known_read_at",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff_2"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

result = (
    with_details
    .select(
        "id",
        F.col("site_installed_kw").alias("m_site_installed_kw"),
        F.col("voltage").alias("voltage_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_site_installed_kw", "voltage_v"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
