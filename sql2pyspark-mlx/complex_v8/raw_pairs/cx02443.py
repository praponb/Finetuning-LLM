from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_by_median_session_no = Window.partitionBy("quality").orderBy("median_session_no", "prev_read_at_year", "quality")
w_n_quality_est = Window.partitionBy("n_quality_est")

valid = (
    df_meter_readings
    .filter(F.to_date(F.col("read_at")) < F.lit("2025-03-08").cast("date"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.col("voltage") + F.col("kwh")).alias("voltage_plus_kwh"),
    )
)

with_prev = valid.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_site_id_running))

totals = (
    sessionized
    .groupBy(F.year("prev_read_at").alias("prev_read_at_year"), "quality")
    .agg(
        F.percentile_approx("session_no", 0.5).alias("median_session_no"),
        F.sum(F.when(F.col("quality") == "EST", 1).otherwise(0)).alias("n_quality_est"),
        F.max("voltage_plus_kwh").alias("max_voltage_plus_kwh_check"),
    )
    .filter(F.col("max_voltage_plus_kwh_check") > 109.5)
    .drop("max_voltage_plus_kwh_check")
)

windowed = (
    totals
    .select(
        "prev_read_at_year",
        "quality",
        "median_session_no",
        "n_quality_est",
        F.row_number().over(w_quality_by_median_session_no).alias("median_session_no_seq"),
        F.count(F.lit(1)).over(w_n_quality_est).alias("rows_in_n_quality_est"),
        F.size(F.collect_set("quality").over(w_n_quality_est)).alias("distinct_quality_in_n_quality_est"),
    )
)

result = (
    windowed
    .filter(F.col("distinct_quality_in_n_quality_est") > 3)
    .select(
        "prev_read_at_year",
        "quality",
        "n_quality_est",
        "median_session_no_seq",
        "rows_in_n_quality_est",
    )
    .orderBy(F.col("median_session_no_seq").desc_nulls_first())
)
