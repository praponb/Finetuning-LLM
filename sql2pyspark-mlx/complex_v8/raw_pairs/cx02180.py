from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_site_tariff_site_id_by_volta = Window.partitionBy("site_tariff", "site_id").orderBy("voltage", "id").rowsBetween(-3, -1)
w_site_tariff_by_kwh = Window.partitionBy("site_tariff").orderBy("kwh", "id")
w_site_id_by_voltage_running = Window.partitionBy("site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_tariff_site_id_by_volta_2 = Window.partitionBy("site_tariff", "site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_site_tariff_by_kwh_p = Window.partitionBy("site_id", "site_tariff").orderBy("kwh", "id").rowsBetween(-6, -1)
w_site_id_by_voltage_full = Window.partitionBy("site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_meter_readings
    .filter(~F.col("quality").isin("OK", "BAD"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
    )
)

with_details = (
    prepared.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        F.col("si.tariff").alias("site_tariff"),
    )
)

with_windows = (
    with_details
    .select(
        "*",
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.round(F.avg("voltage").over(w_site_tariff_site_id_by_volta), 2).alias("voltage_avg_3_prior"),
        F.lag("kwh").over(w_site_tariff_by_kwh).alias("prev_kwh"),
        F.ntile(10).over(w_site_tariff_by_kwh).alias("kwh_decile"),
        F.size(F.collect_set("site_tariff").over(w_site_id)).alias("distinct_site_tariff_in_site_id"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id), F.lit(0)), 4).alias("kwh_share_of_site_id"),
        F.last("read_at", ignorenulls=True).over(w_site_id_by_voltage_running).alias("last_known_read_at"),
        F.max("kwh").over(w_site_id).alias("site_id_max_kwh"),
        F.first("voltage").over(w_site_tariff_site_id_by_volta_2).alias("first_voltage"),
        F.max("kwh").over(w_site_id_site_tariff_by_kwh_p).alias("kwh_max_6_prior"),
        F.first("read_at").over(w_site_id_by_voltage_full).alias("first_read_at"),
    )
)

stats = (
    with_windows
    .groupBy(
        "kwh_decile",
        F.date_format("last_known_read_at", "yyyy-MM").alias("last_known_read_at_ym"),
        F.date_format("read_at", "yyyy-MM").alias("read_at_ym"),
    )
    .agg(
        F.min("first_read_at").alias("first_first_read_at"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    stats
    .select(
        "kwh_decile",
        "last_known_read_at_ym",
        "read_at_ym",
        "first_first_read_at",
    )
    .orderBy("read_at_ym")
)
