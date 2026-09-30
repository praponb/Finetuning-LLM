from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month = Window.partitionBy(F.month("read_at"))
w_all_by_read_at_full = Window.orderBy(F.col("read_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_read_at_running = Window.orderBy(F.col("read_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_meter_readings
    .filter(F.col("voltage") <= 232.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

with_trends = (
    cleaned
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.sum("voltage").over(w_read_at_month).alias("read_at_month_sum_voltage"),
        F.first("voltage").over(w_all_by_read_at_full).alias("first_voltage"),
        F.max("voltage").over(w_all_by_read_at_running).alias("max_voltage_so_far"),
        F.last("read_at").over(w_all_by_read_at_full).alias("last_read_at"),
    )
)

grouped = (
    with_trends
    .filter(F.hour(F.col("read_at")).between(8, 13))
    .groupBy(
        F.when(F.col("first_voltage") >= 240.1, "A").when(F.col("first_voltage") >= 222.1, "B").otherwise("C").alias("first_voltage_tier"),
        "site_id",
        "quality",
    )
    .agg(
        F.round(F.sum("max_voltage_so_far"), 2).alias("total_max_voltage_so_far"),
        F.max("read_at").alias("last_read_at_2"),
        F.round(F.sum("first_voltage") / F.nullif(F.sum("read_at_month_sum_voltage"), F.lit(0)), 3).alias("first_voltage_to_read_at_month_sum_voltage_ratio"),
        F.avg("read_at_month_sum_voltage").alias("avg_read_at_month_sum_voltage_check"),
    )
    .filter(F.col("avg_read_at_month_sum_voltage_check") > 994.0)
    .drop("avg_read_at_month_sum_voltage_check")
)

result = (
    grouped
    .groupBy("site_id")
    .pivot("first_voltage_tier", ["A", "B"])
    .agg(F.count("total_max_voltage_so_far"))
    .orderBy("site_id")
)
