from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_to_date_quality_or_u = Window.partitionBy(F.to_date("read_at"), "quality_or_unknown")
w_read_at_month_by_kwh_moving = Window.partitionBy(F.month("read_at")).orderBy("kwh", "id").rowsBetween(-3, Window.currentRow)
w_all_by_voltage_running = Window.orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_month_by_kwh_prior_1 = Window.partitionBy(F.month("read_at")).orderBy("kwh", "id").rowsBetween(-1, -1)
w_read_at_month_by_kwh = Window.partitionBy(F.month("read_at")).orderBy("kwh", "id")
w_all_by_kwh = Window.orderBy(F.col("kwh").desc(), "id")

filtered_readings = (
    df_meter_readings
    .filter((~F.col("quality").isin("EST", "BAD")) & (F.col("kwh") >= 14.4))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

combined_details = (
    filtered_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") <= 373.3), "left")
    .select("s.*", "si.site_name")
)

windowed = (
    combined_details
    .select(
        "*",
        F.size(F.collect_set("quality").over(w_read_at_to_date_quality_or_u)).alias("distinct_quality_in_read_at_to_date_quality_or_u"),
        F.round(F.avg("voltage").over(w_read_at_month_by_kwh_moving), 2).alias("voltage_avg_4"),
        F.first("voltage", ignorenulls=True).over(w_all_by_voltage_running).alias("first_known_voltage"),
        F.max("voltage").over(w_read_at_month_by_kwh_prior_1).alias("voltage_max_1_prior"),
        F.lag("voltage", 2).over(w_read_at_month_by_kwh).alias("prev2_voltage"),
        F.lag("voltage", 2).over(w_all_by_kwh).alias("prev2_voltage_2"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_read_at_month_by_kwh))) / 60, 1).alias("minutes_since_prev_read_at"),
    )
)

result = (
    windowed
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "site_name",
        "voltage_avg_4",
        "first_known_voltage",
        "voltage_max_1_prior",
        "prev2_voltage_2",
        "minutes_since_prev_read_at",
    )
    .orderBy(F.col("id").desc(), F.col("site_id").desc())
)
