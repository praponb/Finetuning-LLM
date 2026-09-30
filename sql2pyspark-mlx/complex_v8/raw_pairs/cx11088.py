from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_quality_len_full = Window.orderBy("quality_len", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_kwh_running = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_quality_len_by_read = Window.partitionBy("site_id", "quality_len").orderBy("read_at", "id")

valid = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality_len",
        F.nth_value("voltage", 2).over(w_all_by_quality_len_full).alias("second_voltage"),
        F.last("kwh", ignorenulls=True).over(w_all_by_kwh_running).alias("last_known_kwh"),
        F.lag("voltage", 2, 0).over(w_site_id_quality_len_by_read).alias("prev2_voltage"),
    )
)

screened = (
    metrics.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") < 78.6), "left_anti")
)

result = (
    screened
    .filter(F.col("kwh") <= 85.7)
    .select(
        "id",
        "read_at",
        "kwh",
        "quality_len",
        "last_known_kwh",
        "prev2_voltage",
    )
    .orderBy(F.col("id").asc(), F.col("kwh").desc())
)
