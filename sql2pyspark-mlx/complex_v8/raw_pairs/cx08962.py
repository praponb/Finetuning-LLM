from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_voltage_ties = Window.partitionBy("quality").orderBy(F.col("voltage").asc_nulls_last())
w_quality = Window.partitionBy("quality")
w_quality_by_kwh_distance_runn = Window.partitionBy("quality").orderBy("kwh_distance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_voltage_dense_rank = Window.partitionBy("quality", "voltage_dense_rank")
w_voltage_dense_rank_quality_b = Window.partitionBy("voltage_dense_rank", "quality").orderBy("voltage", "id")
w_voltage_dense_rank_quality_b_2 = Window.partitionBy("voltage_dense_rank", "quality").orderBy(F.col("last_known_voltage").desc(), "id")

filtered_readings = (
    df_meter_readings
    .filter(F.col("read_at") < F.lit("2025-02-12 08:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.min("read_at").alias("first_reading_read_at"))
)

with_stats = (
    filtered_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.first_reading_read_at")
)

derived = (
    with_stats
    .select(
        "id",
        "kwh",
        "voltage",
        "quality",
        F.abs(F.col("kwh") - 63.6).alias("kwh_distance"),
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
    )
)

with_windows = (
    derived
    .select(
        "*",
        F.dense_rank().over(w_quality_by_voltage_ties).alias("voltage_dense_rank"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality), 2).alias("voltage_vs_quality_avg"),
        F.last("voltage", ignorenulls=True).over(w_quality_by_kwh_distance_runn).alias("last_known_voltage"),
        F.round(F.col("kwh_bucket") / F.nullif(F.sum("kwh_bucket").over(w_quality), F.lit(0)), 4).alias("kwh_bucket_share_of_quality"),
        F.round(F.col("kwh_distance") / F.nullif(F.sum("kwh_distance").over(w_quality), F.lit(0)), 4).alias("kwh_distance_share_of_quality"),
    )
)

windowed = (
    with_windows
    .select(
        "id",
        "voltage",
        "quality",
        "kwh_distance",
        "kwh_bucket",
        "kwh_distance_share_of_quality",
        F.dense_rank().over(w_voltage_dense_rank_quality_b).alias("voltage_dense_rank_2"),
        F.round(F.avg("voltage_dense_rank").over(w_quality_voltage_dense_rank), 2).alias("quality_voltage_dense_rank_avg_voltage_dense_ran"),
        F.count(F.lit(1)).over(w_quality_voltage_dense_rank).alias("rows_in_quality_voltage_dense_rank"),
        F.rank().over(w_voltage_dense_rank_quality_b_2).alias("last_known_voltage_rank"),
        F.row_number().over(w_voltage_dense_rank_quality_b).alias("seq_no"),
    )
)

tagged = (
    windowed
    .filter(F.col("kwh_distance_share_of_quality").between(0.2, 0.7))
    .select(
        "*",
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
        F.greatest(F.col("voltage_dense_rank_2"), F.col("kwh_distance")).alias("greatest_voltage_dense_rank_2_kwh_distance"),
        F.round(F.col("voltage") * 0.25, 2).alias("voltage_adj"),
    )
)

result = (
    tagged
    .filter(F.col("voltage") > 220.2)
    .select(
        "id",
        "quality",
        "kwh_distance",
        "kwh_distance_share_of_quality",
        "voltage_dense_rank_2",
        "quality_voltage_dense_rank_avg_voltage_dense_ran",
        "rows_in_quality_voltage_dense_rank",
        "last_known_voltage_rank",
        "seq_no",
        "voltage_filled",
        "greatest_voltage_dense_rank_2_kwh_distance",
        F.lower(F.trim(F.col("quality"))).alias("quality_clean"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
    .orderBy("rows_in_quality_voltage_dense_rank")
)
