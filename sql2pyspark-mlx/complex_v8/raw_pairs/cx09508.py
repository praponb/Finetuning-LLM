from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality")
w_all_by_least_kwh_voltage = Window.orderBy(F.col("least_kwh_voltage").desc(), "id")
w_quality_site_id_by_voltage_d = Window.partitionBy("quality", "site_id").orderBy(F.col("voltage_distance").desc(), "id")

base_readings = (
    df_meter_readings
    .filter(F.col("site_id") <= 7)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.abs(F.col("voltage") - 221.8).alias("voltage_distance"),
        F.least(F.col("kwh"), F.col("voltage")).alias("least_kwh_voltage"),
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
    )
)

with_windows = (
    base_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "voltage_distance",
        "least_kwh_voltage",
        "voltage_per_kwh",
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
        F.row_number().over(w_all_by_least_kwh_voltage).alias("seq_no"),
        F.sum("voltage_distance").over(w_quality).alias("quality_sum_voltage_distance"),
        F.row_number().over(w_quality_site_id_by_voltage_d).alias("voltage_distance_seq"),
    )
)

with_parent = (
    with_windows.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "s.voltage_per_kwh",
        "s.rows_in_quality",
        "s.seq_no",
        "s.quality_sum_voltage_distance",
        F.col("si.zone").alias("site_zone"),
    )
)

result = (
    with_parent
    .groupBy("seq_no", "quality")
    .pivot("site_zone", ["Z3", "Z4"])
    .agg(F.sum("rows_in_quality").alias("amount"), F.max("rows_in_quality").alias("max_v"))
)
