from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_voltage = Window.orderBy(F.col("voltage").desc(), "id")
w_all_by_voltage_running = Window.orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_quality = Window.partitionBy("site_id", "quality")

filtered_readings = (
    df_meter_readings
    .filter((F.col("quality") != "EST") | (F.col("kwh") > 46.6))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

sequenced = (
    filtered_readings
    .select(
        "*",
        F.lag("voltage", 3).over(w_all_by_voltage).alias("prev3_voltage"),
        F.last("read_at", ignorenulls=True).over(w_all_by_voltage_running).alias("last_known_read_at"),
        F.count(F.lit(1)).over(w_site_id_quality).alias("rows_in_site_id_quality"),
        F.max("kwh").over(w_all_by_voltage_running).alias("max_kwh_so_far"),
        F.round(F.avg("kwh").over(w_site_id_quality), 2).alias("site_id_quality_avg_kwh"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["site_id", "quality"], ["site_id"], ["quality"], []],
        "site_id", "quality",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count("*").alias("row_count"),
    )
)
