from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality")
w_all_by_kwh_running = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_kwh = Window.orderBy(F.col("kwh").desc(), "id")
w_quality_by_kwh_ties = Window.partitionBy("quality").orderBy("kwh")
w_quality_by_kwh = Window.partitionBy("quality").orderBy("kwh", "id")
w_voltage_tier_by_kwh = Window.partitionBy("voltage_tier").orderBy(F.col("kwh").desc_nulls_first(), "id")
w_quality_by_kwh_full = Window.partitionBy("quality").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_meter_readings
    .filter((F.col("voltage") <= 239.6) & (F.col("quality") != "BAD"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
        F.when(F.col("voltage") >= 231.6, "A").when(F.col("voltage") >= 223.1, "B").otherwise("C").alias("voltage_tier"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.sum("kwh").over(w_quality).alias("quality_sum_kwh"),
        F.first("kwh", ignorenulls=True).over(w_all_by_kwh_running).alias("first_known_kwh"),
        F.round(F.percent_rank().over(w_all_by_kwh), 3).alias("kwh_percent_rank"),
        F.dense_rank().over(w_quality_by_kwh_ties).alias("kwh_dense_rank"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_quality_by_kwh))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.ntile(4).over(w_voltage_tier_by_kwh).alias("kwh_quartile"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
        F.lag("read_at").over(w_quality_by_kwh).alias("prev_read_at"),
        F.nth_value("kwh", 2).over(w_quality_by_kwh_full).alias("second_kwh"),
        F.row_number().over(w_all_by_kwh).alias("rn"),
    )
)

first_rows = (
    with_windows
    .filter(F.col("kwh_dense_rank") <= 3)
    .drop("kwh_dense_rank")
)

result = (
    first_rows
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage_tier",
        "quality_sum_kwh",
        "first_known_kwh",
        "kwh_percent_rank",
        "minutes_since_prev_read_at",
        "rows_in_quality",
        "prev_read_at",
        "rn",
    )
)
