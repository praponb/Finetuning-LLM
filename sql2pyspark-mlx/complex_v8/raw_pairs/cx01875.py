from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_voltage_moving_6 = Window.orderBy("voltage", "id").rowsBetween(-6, Window.currentRow)
w_kwh_tier_quality = Window.partitionBy("kwh_tier", "quality")
w_all_by_read_at_running = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_all_by_voltage = Window.orderBy("voltage", "id")
w_kwh_tier_by_read_at_full = Window.partitionBy("kwh_tier").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.when(F.col("kwh") >= 106.0, "large").when(F.col("kwh") >= 28.4, "medium").otherwise("small").alias("kwh_tier"),
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
    )
)

ranked = (
    base_readings
    .select(
        "*",
        F.round(F.avg("voltage").over(w_all_by_voltage_moving_6), 2).alias("voltage_avg_7"),
        F.sum("voltage").over(w_kwh_tier_quality).alias("kwh_tier_quality_sum_voltage"),
        F.last("voltage", ignorenulls=True).over(w_all_by_read_at_running).alias("last_known_voltage"),
        F.size(F.collect_set("kwh_tier").over(w_quality_site_id)).alias("distinct_kwh_tier_in_quality_site_id"),
        F.size(F.collect_set("site_id").over(w_kwh_tier_quality)).alias("distinct_site_id_in_kwh_tier_quality"),
        F.ntile(4).over(w_all_by_voltage).alias("voltage_quartile"),
        F.dense_rank().over(w_all_by_voltage).alias("voltage_dense_rank"),
        F.nth_value("voltage", 2).over(w_kwh_tier_by_read_at_full).alias("second_voltage"),
    )
)

result = (
    ranked
    .select(
        "id",
        "read_at",
        "voltage",
        "quality",
        "kwh_tier",
        "read_at_label",
        "voltage_avg_7",
        "kwh_tier_quality_sum_voltage",
        "last_known_voltage",
        "distinct_site_id_in_kwh_tier_quality",
        "voltage_dense_rank",
        "second_voltage",
        F.greatest(F.col("voltage_avg_7"), F.col("second_voltage")).alias("greatest_voltage_avg_7_second_voltage"),
    )
    .orderBy(
        F.col("voltage_dense_rank").asc_nulls_last(),
        F.col("last_known_voltage").asc_nulls_last(),
    )
)
