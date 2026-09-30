from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_kwh = Window.orderBy("kwh", "id")
w_site_id_quality_by_kwh_full = Window.partitionBy("site_id", "quality").orderBy(F.col("kwh").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_voltage_running = Window.orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_kwh_ties = Window.orderBy("kwh")
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_site_id = Window.partitionBy("site_id")
w_all_by_kwh_running = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_distinct_quality_in = Window.partitionBy("site_id", "distinct_quality_in_site_id")
w_distinct_quality_in_site_id = Window.partitionBy("distinct_quality_in_site_id", "site_id").orderBy("voltage")
w_distinct_quality_in_site_id_2 = Window.partitionBy("distinct_quality_in_site_id", "site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_voltage_ties = Window.orderBy(F.col("voltage").desc())
w_quality_distinct_quality_in = Window.partitionBy("quality", "distinct_quality_in_site_id")

prepared_readings = (
    df_meter_readings
    .filter(F.col("quality") != "BAD")
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

with_windows = (
    prepared_readings
    .select(
        "*",
        F.rank().over(w_all_by_kwh).alias("kwh_rank"),
        F.last("read_at").over(w_site_id_quality_by_kwh_full).alias("last_read_at"),
        F.last("voltage", ignorenulls=True).over(w_all_by_voltage_running).alias("last_known_voltage"),
        F.round(F.percent_rank().over(w_all_by_kwh_ties), 3).alias("kwh_percent_rank"),
        F.max("voltage").over(w_quality_site_id).alias("quality_site_id_max_voltage"),
        F.size(F.collect_set("quality").over(w_site_id)).alias("distinct_quality_in_site_id"),
        F.first("read_at", ignorenulls=True).over(w_all_by_kwh_running).alias("first_known_read_at"),
        F.first("read_at", ignorenulls=True).over(w_all_by_voltage_running).alias("first_known_read_at_2"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        "quality_clean",
        "last_read_at",
        "last_known_voltage",
        "quality_site_id_max_voltage",
        "distinct_quality_in_site_id",
        "first_known_read_at",
        F.round(F.col("distinct_quality_in_site_id") / F.nullif(F.sum("distinct_quality_in_site_id").over(w_site_id_distinct_quality_in), F.lit(0)), 4).alias("distinct_quality_in_site_id_share_of_site_id_dis"),
        F.rank().over(w_distinct_quality_in_site_id).alias("voltage_rank"),
        F.nth_value("first_known_read_at_2", 2).over(w_distinct_quality_in_site_id_2).alias("second_first_known_read_at_2"),
        F.dense_rank().over(w_all_by_voltage_ties).alias("voltage_dense_rank"),
        F.count(F.lit(1)).over(w_quality_distinct_quality_in).alias("rows_in_quality_distinct_quality_in_site_id"),
    )
)

result = (
    metrics
    .select(
        "id",
        "site_id",
        "quality",
        "quality_clean",
        "last_read_at",
        "last_known_voltage",
        "quality_site_id_max_voltage",
        "distinct_quality_in_site_id",
        "first_known_read_at",
        "distinct_quality_in_site_id_share_of_site_id_dis",
        "voltage_dense_rank",
        "rows_in_quality_distinct_quality_in_site_id",
        F.abs(F.col("kwh") - 63.6).alias("kwh_distance"),
        F.length(F.trim(F.col("quality_clean"))).alias("quality_clean_len"),
    )
    .orderBy(
        F.col("distinct_quality_in_site_id").desc(),
        F.col("kwh_distance").desc(),
    )
)
