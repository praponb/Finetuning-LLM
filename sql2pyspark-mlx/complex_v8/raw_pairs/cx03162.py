from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality_by_voltage = Window.partitionBy("site_id", "quality").orderBy(F.col("voltage").desc_nulls_first(), "id")
w_quality = Window.partitionBy("quality")
w_site_id_quality_by_voltage_m = Window.partitionBy("site_id", "quality").orderBy(F.col("voltage").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_read_at_running = Window.orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_voltage_tercile_quality = Window.partitionBy("voltage_tercile", "quality")
w_quality_quality_group_by_vol = Window.partitionBy("quality", "quality_group").orderBy("voltage_tercile", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_quality_group_by_vol_2 = Window.partitionBy("quality", "quality_group").orderBy("voltage_tercile", "id")

scoped = (
    df_meter_readings
    .filter((F.month(F.col("read_at")).isin(2, 3)) & (F.col("voltage") > 216.1))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

windowed = (
    scoped
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        F.ntile(3).over(w_site_id_quality_by_voltage).alias("voltage_tercile"),
        F.max("kwh").over(w_quality).alias("quality_max_kwh"),
        F.size(F.collect_set("site_id").over(w_quality)).alias("distinct_site_id_in_quality"),
        F.sum("kwh").over(w_site_id_quality_by_voltage_m).alias("kwh_sum_3"),
        F.last("voltage", ignorenulls=True).over(w_all_by_read_at_running).alias("last_known_voltage"),
        F.max("kwh").over(w_all_by_read_at_running).alias("max_kwh_so_far"),
    )
)

scored = (
    windowed
    .select(
        "id",
        "quality",
        "voltage_tercile",
        "quality_max_kwh",
        "kwh_sum_3",
        "last_known_voltage",
        "max_kwh_so_far",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.when(F.col("quality").isin("OK", "EST"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
    )
)

with_trends = (
    scored
    .select(
        "id",
        "quality",
        "last_known_voltage",
        "quality_slug",
        F.count(F.lit(1)).over(w_voltage_tercile_quality).alias("rows_in_voltage_tercile_quality"),
        F.first("max_kwh_so_far", ignorenulls=True).over(w_quality_quality_group_by_vol).alias("first_known_max_kwh_so_far"),
        F.round(F.col("last_known_voltage") - F.avg("last_known_voltage").over(w_voltage_tercile_quality), 2).alias("last_known_voltage_vs_voltage_tercile_quality_av"),
        F.ntile(4).over(w_quality_quality_group_by_vol_2).alias("voltage_tercile_quartile"),
        F.lead("max_kwh_so_far").over(w_quality_quality_group_by_vol_2).alias("next_max_kwh_so_far"),
        F.min("kwh_sum_3").over(w_voltage_tercile_quality).alias("voltage_tercile_quality_min_kwh_sum_3"),
    )
)

result = (
    with_trends
    .filter(F.col("last_known_voltage").between(220.2, 240.2))
    .select(
        "id",
        "quality",
        "last_known_voltage",
        "quality_slug",
        "rows_in_voltage_tercile_quality",
        "last_known_voltage_vs_voltage_tercile_quality_av",
        "voltage_tercile_quartile",
        "next_max_kwh_so_far",
        "voltage_tercile_quality_min_kwh_sum_3",
        (F.col("next_max_kwh_so_far") + F.col("last_known_voltage")).alias("next_max_kwh_so_far_plus_last_known_voltage"),
        F.concat_ws(": ", F.col("quality"), F.col("quality_slug")).alias("quality_quality_slug_label"),
    )
    .orderBy(
        F.col("voltage_tercile_quartile").asc_nulls_last(),
        F.col("last_known_voltage").asc(),
        "id",
    )
    .limit(5)
)
