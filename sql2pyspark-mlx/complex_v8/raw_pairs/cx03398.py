from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_voltage = Window.orderBy("voltage", "id")
w_site_id_quality_by_read_at_r = Window.partitionBy("site_id", "quality").orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_quality_by_read_at = Window.partitionBy("site_id", "quality").orderBy(F.col("read_at").desc(), "id")
w_all_by_voltage_running = Window.orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_meter_readings
    .filter((F.hour(F.col("read_at")).between(9, 11)) & (F.col("voltage") >= 220.2))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.least(F.col("kwh"), F.col("voltage")).alias("least_kwh_voltage"),
    )
)

with_details = (
    valid.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.length(F.trim(F.col("si.site_name"))) >= 8)
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.least_kwh_voltage",
        "si.site_name",
    )
)

windowed = (
    with_details
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "least_kwh_voltage",
        F.round(F.cume_dist().over(w_all_by_voltage), 3).alias("voltage_cume_dist"),
        F.first("kwh", ignorenulls=True).over(w_site_id_quality_by_read_at_r).alias("first_known_kwh"),
        F.rank().over(w_all_by_voltage).alias("voltage_rank"),
        F.ntile(5).over(w_all_by_voltage).alias("voltage_quintile"),
        F.ntile(5).over(w_site_id_quality_by_read_at).alias("read_at_quintile"),
        F.count(F.lit(1)).over(w_all_by_voltage_running).alias("running_count"),
    )
)

result = (
    windowed
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "least_kwh_voltage",
        "voltage_cume_dist",
        "voltage_rank",
        "read_at_quintile",
        "running_count",
    )
    .orderBy("kwh", "id")
    .limit(5)
)
