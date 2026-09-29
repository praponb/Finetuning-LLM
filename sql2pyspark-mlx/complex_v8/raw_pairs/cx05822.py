from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_zone_read_at_year = Window.partitionBy("site_zone", F.year("read_at"))
w_all_by_kwh_minus_voltage = Window.orderBy(F.col("kwh_minus_voltage").desc(), "id")
w_site_id_by_voltage_distance = Window.partitionBy("site_id").orderBy("voltage_distance", "id")
w_site_id_quality_by_voltage_d = Window.partitionBy("site_id", "quality").orderBy(F.col("voltage_distance").desc(), "id")
w_site_zone = Window.partitionBy("site_zone")
w_all_by_kwh_minus_voltage_2 = Window.orderBy("kwh_minus_voltage", "id")
w_site_id_site_zone_by_kwh_min = Window.partitionBy("site_id", "site_zone").orderBy(F.col("kwh_minus_voltage").desc(), "id")
w_site_id_quality_by_voltage_d_2 = Window.partitionBy("site_id", "quality").orderBy(F.col("voltage_distance").desc(), "id").rowsBetween(-2, -1)
w_site_id_quality = Window.partitionBy("site_id", "quality")

base_readings = (
    df_meter_readings
    .filter(F.col("quality").isin("OK", "BAD"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.col("kwh") - F.col("voltage")).alias("kwh_minus_voltage"),
        F.abs(F.col("voltage") - 222.5).alias("voltage_distance"),
    )
)

joined = (
    base_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on") >= F.lit("2024-08-24").cast("date")), "inner")
    .filter(F.col("si.zone").isin("Z4", "Z2"))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "s.kwh_minus_voltage",
        "s.voltage_distance",
        F.col("si.zone").alias("site_zone"),
        "si.site_name",
    )
)

windowed = (
    joined
    .select(
        "id",
        "read_at",
        "voltage_distance",
        "site_name",
        F.round(F.avg("voltage_distance").over(w_site_zone_read_at_year), 2).alias("site_zone_read_at_year_avg_voltage_distance"),
        F.lag("kwh_minus_voltage", 3).over(w_all_by_kwh_minus_voltage).alias("prev3_kwh_minus_voltage"),
        F.ntile(10).over(w_site_id_by_voltage_distance).alias("voltage_distance_decile"),
        F.ntile(3).over(w_site_id_quality_by_voltage_d).alias("voltage_distance_tercile"),
        F.max("kwh_minus_voltage").over(w_site_zone).alias("site_zone_max_kwh_minus_voltage"),
        F.ntile(5).over(w_all_by_kwh_minus_voltage_2).alias("kwh_minus_voltage_quintile"),
        F.round(F.percent_rank().over(w_site_id_site_zone_by_kwh_min), 3).alias("kwh_minus_voltage_percent_rank"),
        F.max("voltage_distance").over(w_site_id_quality_by_voltage_d_2).alias("voltage_distance_max_2_prior"),
        F.round(F.col("kwh_minus_voltage") / F.nullif(F.sum("kwh_minus_voltage").over(w_site_zone), F.lit(0)), 4).alias("kwh_minus_voltage_share_of_site_zone"),
        F.round(F.col("voltage_distance") - F.avg("voltage_distance").over(w_site_zone_read_at_year), 2).alias("voltage_distance_vs_site_zone_read_at_year_avg"),
        F.min("kwh_minus_voltage").over(w_site_id_quality).alias("site_id_quality_min_kwh_minus_voltage"),
        F.row_number().over(w_all_by_kwh_minus_voltage).alias("seq_no"),
    )
)

top_rows = (
    windowed
    .filter(F.col("seq_no") <= 5)
    .drop("seq_no")
)

result = (
    top_rows
    .select(
        "id",
        "read_at",
        "site_name",
        "site_zone_read_at_year_avg_voltage_distance",
        "voltage_distance_tercile",
        "site_zone_max_kwh_minus_voltage",
        "kwh_minus_voltage_quintile",
        "voltage_distance_max_2_prior",
        "kwh_minus_voltage_share_of_site_zone",
        F.when(F.col("voltage_distance") >= 183.8, "HIGH").when(F.col("voltage_distance") >= 61.2, "MEDIUM").otherwise("LOW").alias("voltage_distance_tier"),
    )
)
