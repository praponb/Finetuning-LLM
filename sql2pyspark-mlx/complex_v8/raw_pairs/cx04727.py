from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone = Window.partitionBy("zone")
w_zone_site_id_by_read_at = Window.partitionBy("zone", "site_id").orderBy("read_at", "id")
w_zone_site_id_by_read_at_ties = Window.partitionBy("zone", "site_id").orderBy("read_at")
w_zone_site_id_by_read_at_full = Window.partitionBy("zone", "site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

in_window = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "inner",
    )
    .select(
        "r.id",
        "r.site_id",
        "r.read_at",
        "r.kwh",
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("read_at").alias("first_reading_read_at"),
        F.max("read_at").alias("last_reading_read_at"),
    )
)

with_stats = (
    in_window.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.first_reading_read_at", "m.last_reading_read_at")
)

qualified = (
    with_stats.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
    .filter(F.col("energy_cost").between(180.0, 765.0))
)

with_windows = (
    qualified
    .select(
        "*",
        F.round(F.avg("kwh").over(w_zone), 2).alias("zone_avg_kwh"),
        (F.col("rate") - F.lag("rate").over(w_zone_site_id_by_read_at)).alias("rate_change"),
        F.row_number().over(w_zone_site_id_by_read_at).alias("read_at_seq"),
        F.round(F.percent_rank().over(w_zone_site_id_by_read_at_ties), 3).alias("read_at_percent_rank"),
        F.sum("rate").over(w_zone).alias("zone_sum_rate"),
        F.nth_value("voltage", 2).over(w_zone_site_id_by_read_at_full).alias("second_voltage"),
    )
)

result = (
    with_windows
    .filter(F.col("rate") <= 7.6)
    .select(
        "id",
        "site_id",
        "voltage",
        "zone",
        "first_reading_read_at",
        "zone_avg_kwh",
        "rate_change",
        "read_at_seq",
        "read_at_percent_rank",
        "zone_sum_rate",
        "second_voltage",
        F.date_trunc("year", F.col("last_reading_read_at")).alias("last_reading_read_at_year"),
        F.to_date(F.col("first_reading_read_at")).alias("first_reading_read_at_date"),
    )
    .orderBy(F.col("last_reading_read_at_year").desc(), "id")
)
