from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_plus_30m_to_date_sit = Window.partitionBy(F.to_date("read_at_plus_30m"), "site_id")
w_read_at_month_site_id_by_rea = Window.partitionBy(F.date_trunc("month", "read_at"), "site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_month_site_id_by_rea_2 = Window.partitionBy(F.date_trunc("month", "read_at"), "site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_voltage = Window.partitionBy("site_id").orderBy("voltage", "id")
w_site_id = Window.partitionBy("site_id")
w_site_id_by_voltage_ties = Window.partitionBy("site_id").orderBy("voltage")

cleaned_readings = (
    df_meter_readings
    .filter((F.col("kwh").between(37.1, 90.6)) & (F.month(F.col("read_at")).isin(1, 3)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        (F.col("read_at") + F.expr("INTERVAL 30 MINUTES")).alias("read_at_plus_30m"),
    )
)

metrics = (
    cleaned_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "read_at_plus_30m",
        F.nth_value("read_at_plus_30m", 2).over(w_read_at_month_site_id_by_rea).alias("second_read_at_plus_30m"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_read_at_plus_30m_to_date_sit), F.lit(0)), 4).alias("kwh_share_of_read_at_plus_30m_to_date_site_id"),
        F.last("read_at", ignorenulls=True).over(w_read_at_month_site_id_by_rea_2).alias("last_known_read_at"),
    )
)

enriched_site = (
    metrics.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select("s.*", "si.site_name")
)

screened = (
    enriched_site.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality") == "OK"), "left_anti")
)

with_trends = (
    screened
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "read_at_plus_30m",
        "second_read_at_plus_30m",
        "last_known_read_at",
        "site_name",
        F.ntile(4).over(w_site_id_by_voltage).alias("voltage_quartile"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id), 2).alias("voltage_vs_site_id_avg"),
        F.dense_rank().over(w_site_id_by_voltage_ties).alias("voltage_dense_rank"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "site_id",
        "voltage",
        "read_at_plus_30m",
        "last_known_read_at",
        "site_name",
        "voltage_quartile",
        (F.col("second_read_at_plus_30m") + F.expr("INTERVAL 30 MINUTES")).alias("second_read_at_plus_30m_plus_30m"),
    )
    .orderBy(F.col("id").desc())
)
