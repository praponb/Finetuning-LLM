from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality = Window.partitionBy("site_id", "quality")
w_quality_by_read_at = Window.partitionBy("quality").orderBy(F.col("read_at").desc(), "id")
w_all_by_voltage_full = Window.orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_voltage_running = Window.orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_by_read_at_ties = Window.partitionBy("quality").orderBy(F.col("read_at").desc())

base = (
    df_meter_readings
    .filter((F.col("site_id") <= 6) & (F.col("voltage") < 241.9))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
        F.abs(F.col("kwh") - 86.0).alias("kwh_distance"),
    )
)

metrics = (
    base
    .select(
        "id",
        "kwh",
        "voltage",
        "quality",
        "quality_ref",
        "quality_clean",
        "kwh_distance",
        F.count(F.lit(1)).over(w_site_id_quality).alias("rows_in_site_id_quality"),
        F.round(F.cume_dist().over(w_quality_by_read_at), 3).alias("read_at_cume_dist"),
        F.nth_value("kwh", 2).over(w_all_by_voltage_full).alias("second_kwh"),
        F.count(F.lit(1)).over(w_all_by_voltage_running).alias("running_count"),
        F.round(F.percent_rank().over(w_quality_by_read_at_ties), 3).alias("read_at_percent_rank"),
        F.rank().over(w_quality_by_read_at).alias("read_at_rank"),
    )
)

screened = (
    metrics
    .crossJoin(metrics.agg(F.max("rows_in_site_id_quality").alias("overall_max_rows_in_site_id_quality")))
    .filter(F.col("rows_in_site_id_quality") < F.col("overall_max_rows_in_site_id_quality"))
    .drop("overall_max_rows_in_site_id_quality")
)

result = (
    screened
    .select(
        "id",
        "kwh",
        "voltage",
        "quality_clean",
        "kwh_distance",
        "rows_in_site_id_quality",
        "read_at_cume_dist",
        "second_kwh",
        "running_count",
        "read_at_percent_rank",
        "read_at_rank",
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
        F.length(F.trim(F.col("quality_clean"))).alias("quality_clean_len"),
    )
    .orderBy(F.col("read_at_cume_dist").desc())
)
