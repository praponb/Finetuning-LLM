from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_kwh = Window.partitionBy("site_id").orderBy("kwh", "id")
w_site_id_by_kwh_full = Window.partitionBy("site_id").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_read_at_to_date = Window.partitionBy("quality", F.to_date("read_at"))

filtered_readings = (
    df_meter_readings
    .filter((F.col("kwh") > 15.9) & (F.col("quality") != "EST"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        (F.col("kwh") - F.col("voltage")).alias("kwh_minus_voltage"),
    )
)

tagged = (
    filtered_readings
    .select(
        "*",
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
        F.date_trunc("day", F.col("read_at")).alias("read_at_day"),
    )
)

windowed = (
    tagged
    .select(
        "*",
        F.round((F.unix_timestamp("read_at_day") - F.unix_timestamp(F.lag("read_at_day").over(w_site_id_by_kwh))) / 60, 1).alias("minutes_since_prev_read_at_day"),
        F.nth_value("kwh", 2).over(w_site_id_by_kwh_full).alias("second_kwh"),
        F.size(F.collect_set("site_id").over(w_quality_read_at_to_date)).alias("distinct_site_id_in_quality_read_at_to_date"),
        F.nth_value("voltage_bucket", 2).over(w_site_id_by_kwh_full).alias("second_voltage_bucket"),
        F.count(F.lit(1)).over(w_quality_read_at_to_date).alias("rows_in_quality_read_at_to_date"),
    )
)

result = (
    windowed
    .select(
        "id",
        F.col("kwh").alias("kwh_v"),
        F.col("kwh_minus_voltage").alias("m_kwh_minus_voltage"),
        F.col("minutes_since_prev_read_at_day").alias("m_minutes_since_prev_read_at_day"),
        F.col("second_kwh").alias("m_second_kwh"),
    )
    .unpivot(
        ids=["id"],
        values=["kwh_v", "m_kwh_minus_voltage", "m_minutes_since_prev_read_at_day", "m_second_kwh"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .orderBy("id", "measure")
)
