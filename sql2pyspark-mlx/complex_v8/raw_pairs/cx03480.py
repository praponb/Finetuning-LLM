from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_read_at_to_date = Window.partitionBy("site_id", F.to_date("read_at"))
w_all_by_voltage_running = Window.orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_voltage = Window.orderBy(F.col("voltage").desc(), "id")

filtered_readings = (
    df_meter_readings
    .filter(F.col("read_at") >= F.lit("2025-01-19 13:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
    )
)

with_details_site = (
    filtered_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

screened = (
    with_details_site
    .crossJoin(with_details_site.agg(F.avg("kwh").alias("overall_avg_kwh")))
    .filter(F.col("kwh") > F.col("overall_avg_kwh"))
    .drop("overall_avg_kwh")
)

with_trends = (
    screened
    .select(
        "id",
        "site_id",
        "read_at",
        "site_installed_kw",
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id_read_at_to_date), F.lit(0)), 4).alias("voltage_share_of_site_id_read_at_to_date"),
        F.max("kwh").over(w_all_by_voltage_running).alias("max_kwh_so_far"),
        F.count(F.lit(1)).over(w_site_id_read_at_to_date).alias("rows_in_site_id_read_at_to_date"),
        F.lag("kwh", 2).over(w_all_by_voltage).alias("prev2_kwh"),
    )
)

result = (
    with_trends
    .filter(F.to_date(F.col("read_at")) < F.lit("2025-03-02").cast("date"))
    .select(
        "id",
        "read_at",
        "site_installed_kw",
        "voltage_share_of_site_id_read_at_to_date",
        "max_kwh_so_far",
        "rows_in_site_id_read_at_to_date",
        (F.col("read_at") + F.expr("INTERVAL 30 MINUTES")).alias("read_at_plus_30m"),
    )
    .orderBy(F.col("id").asc_nulls_last(), "read_at")
)
