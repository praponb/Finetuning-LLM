from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_read_at_dayofweek = Window.partitionBy("site_id").orderBy("read_at_dayofweek", "id")
w_site_id_read_at_month_by_rea = Window.partitionBy("site_id", F.date_trunc("month", "read_at")).orderBy("read_at", "id")
w_site_id = Window.partitionBy("site_id")
w_site_id_quality_by_voltage_r = Window.partitionBy("site_id", "quality").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_readings = (
    df_meter_readings
    .filter((F.month(F.col("read_at")).isin(1, 3)) & (F.col("voltage") < 236.7))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.dayofweek(F.col("read_at")).alias("read_at_dayofweek"),
        F.when(F.col("voltage") >= 235.0, "large").when(F.col("voltage") >= 222.1, "medium").otherwise("small").alias("voltage_tier"),
    )
)

qualified = (
    filtered_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.tariff").isin("TOU", "EV")), "left_semi")
)

with_windows = (
    qualified
    .select(
        "*",
        (F.col("read_at_dayofweek") - F.lag("read_at_dayofweek").over(w_site_id_by_read_at_dayofweek)).alias("read_at_dayofweek_change"),
        F.round(F.cume_dist().over(w_site_id_read_at_month_by_rea), 3).alias("read_at_cume_dist"),
        F.max("read_at_dayofweek").over(w_site_id).alias("site_id_max_read_at_dayofweek"),
        F.min("voltage").over(w_site_id_quality_by_voltage_r).alias("min_voltage_so_far"),
    )
)

with_details_site = (
    with_windows.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(~F.col("si.zone").isin("Z2", "Z4"))
    .select(
        "s.*",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone"),
    )
)

result = (
    with_details_site
    .cube("site_zone", "site_id")
    .agg(
        F.round(F.sum("read_at_dayofweek_change") / F.nullif(F.sum("site_installed_kw"), F.lit(0)), 3).alias("read_at_dayofweek_change_to_site_installed_kw_ra"),
        F.round(F.sum("site_id_max_read_at_dayofweek") / F.nullif(F.sum("read_at_dayofweek"), F.lit(0)), 3).alias("site_id_max_read_at_dayofweek_to_read_at_dayofwe"),
    )
    .select(
        F.coalesce(F.col("site_zone"), F.lit("ALL")).alias("site_zone"),
        "site_id",
        "read_at_dayofweek_change_to_site_installed_kw_ra",
        "site_id_max_read_at_dayofweek_to_read_at_dayofwe",
    )
)
