from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id = Window.partitionBy("quality", "site_id")
w_site_id_by_kwh_full = Window.partitionBy("site_id").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_by_voltage_running = Window.partitionBy("site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_voltage_moving_3 = Window.partitionBy("site_id").orderBy("voltage", "id").rowsBetween(-3, Window.currentRow)

filtered_readings = (
    df_meter_readings
    .filter(F.col("kwh") < 89.8)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.to_date(F.col("read_at")).alias("read_at_date"),
    )
)

derived = (
    filtered_readings
    .select(
        "*",
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
        F.date_format(F.col("read_at_date"), "yyyy-MM-dd").alias("read_at_date_label"),
        F.abs(F.col("voltage") - 231.7).alias("voltage_distance"),
    )
)

metrics = (
    derived
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "voltage_filled",
        "voltage_distance",
        F.sum("voltage").over(w_quality_site_id).alias("quality_site_id_sum_voltage"),
        F.nth_value("read_at_date", 2).over(w_site_id_by_kwh_full).alias("second_read_at_date"),
        F.last("voltage_filled", ignorenulls=True).over(w_site_id_by_voltage_running).alias("last_known_voltage_filled"),
        F.sum("kwh").over(w_site_id_by_voltage_moving_3).alias("kwh_sum_4"),
    )
)

with_parent_site = (
    metrics.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.quality",
        "s.voltage_distance",
        "s.second_read_at_date",
        "s.kwh_sum_4",
        F.col("si.zone").alias("site_zone"),
    )
)

result = (
    with_parent_site
    .select("id", "voltage_distance", "kwh_sum_4")
    .unpivot(
        ids=["id"],
        values=["voltage_distance", "kwh_sum_4"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "field")
)
