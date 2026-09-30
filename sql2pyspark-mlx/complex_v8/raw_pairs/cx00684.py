from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality")
w_all_by_kwh_prior_3 = Window.orderBy("kwh", "id").rowsBetween(-3, -1)
w_all_by_kwh_running = Window.orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_kwh = Window.orderBy("kwh", "id")
w_quality_by_last_known_voltag = Window.partitionBy("quality").orderBy(F.col("last_known_voltage").desc(), "id")
w_kwh_decile_quality = Window.partitionBy("kwh_decile", "quality")
w_quality_by_read_at_ties = Window.partitionBy("quality").orderBy("read_at")

recent_readings = df_meter_readings.filter((F.col("quality") != "EST") & (F.col("kwh") <= 106.5))

windowed = (
    recent_readings
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.max("kwh").over(w_quality).alias("quality_max_kwh"),
        F.sum("kwh").over(w_all_by_kwh_prior_3).alias("kwh_sum_3_prior"),
        F.last("voltage", ignorenulls=True).over(w_all_by_kwh_running).alias("last_known_voltage"),
        F.ntile(10).over(w_all_by_kwh).alias("kwh_decile"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality), 2).alias("voltage_vs_quality_avg"),
    )
)

screened = (
    windowed
    .crossJoin(windowed.agg(F.avg("last_known_voltage").alias("overall_avg_last_known_voltage")))
    .filter(F.col("last_known_voltage") > F.col("overall_avg_last_known_voltage"))
    .drop("overall_avg_last_known_voltage")
)

metrics = (
    screened
    .select(
        "*",
        F.lead("read_at", 3).over(w_quality_by_last_known_voltag).alias("next3_read_at"),
        F.lag("kwh", 1, 0).over(w_quality_by_last_known_voltag).alias("prev_kwh"),
        F.round(F.avg("quality_max_kwh").over(w_kwh_decile_quality), 2).alias("kwh_decile_quality_avg_quality_max_kwh"),
    )
)

derived = (
    metrics
    .filter(F.col("next3_read_at") >= F.lit("2025-01-13 15:00:00").cast("timestamp"))
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "kwh_sum_3_prior",
        "kwh_decile",
        "voltage_vs_quality_avg",
        "next3_read_at",
        "kwh_decile_quality_avg_quality_max_kwh",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.to_date(F.col("read_at")).alias("read_at_date"),
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
    )
)

windowed_2 = (
    derived
    .select(
        "id",
        "kwh",
        "voltage",
        "quality",
        "kwh_sum_3_prior",
        "voltage_vs_quality_avg",
        "kwh_decile_quality_avg_quality_max_kwh",
        "quality_slug",
        "read_at_date",
        "quality_prefix",
        F.min("voltage_vs_quality_avg").over(w_kwh_decile_quality).alias("kwh_decile_quality_min_voltage_vs_quality_avg"),
        F.dense_rank().over(w_quality_by_read_at_ties).alias("read_at_dense_rank"),
    )
)

result = (
    windowed_2
    .filter(F.col("voltage_vs_quality_avg") < 49.0)
    .select(
        "id",
        "voltage",
        "quality",
        "kwh_sum_3_prior",
        "voltage_vs_quality_avg",
        "kwh_decile_quality_avg_quality_max_kwh",
        "read_at_date",
        "kwh_decile_quality_min_voltage_vs_quality_avg",
        "read_at_dense_rank",
        F.round(F.col("voltage") * 1.07, 2).alias("voltage_adj"),
    )
    .orderBy("voltage_vs_quality_avg", "id")
    .limit(5)
)
