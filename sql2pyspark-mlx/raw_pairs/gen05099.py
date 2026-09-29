from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("session_id", "id")
w5 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.date_trunc("month", "reading_ts"), "device_code").orderBy("humidity", "id").rowsBetween(-6, -1)
w7 = Window.partitionBy(F.date_trunc("YEAR", "reading_ts"))
w8 = Window.partitionBy(F.date_trunc("month", "reading_ts")).orderBy(F.col("temp_c_avg_6_prior").asc_nulls_last())
w9 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy(F.to_date("reading_ts")).orderBy("last_known_humidity", "id").rowsBetween(-5, -1)
w11 = Window.partitionBy(F.year("reading_ts")).orderBy(F.col("reading_ts").desc(), "id")
w12 = Window.orderBy("temp_c_avg_6_prior_percent_rank_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

flagged = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        F.sum("is_new_session").over(w2).alias("session_id"),
    )
)

enriched = (
    sessionized
    .select(
        "id",
        "reading_ts",
        F.sum("temp_c").over(w3).alias("running_temp_c"),
        F.row_number().over(w4).alias("seq_no"),
        F.last("humidity", ignorenulls=True).over(w5).alias("last_known_humidity"),
        F.avg("temp_c").over(w6).alias("temp_c_avg_6_prior"),
        F.corr("status_bits", "temp_c").over(w7).alias("corr_status_bits_temp_c"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "reading_ts",
        "running_temp_c",
        "seq_no",
        F.percent_rank().over(w8).alias("temp_c_avg_6_prior_percent_rank"),
        F.first("corr_status_bits_temp_c", ignorenulls=True).over(w9).alias("first_known_corr_status_bits_temp_c"),
        F.sum("corr_status_bits_temp_c").over(w9).alias("running_corr_status_bits_temp_c"),
        F.avg("temp_c_avg_6_prior").over(w10).alias("temp_c_avg_6_prior_avg_5_prior"),
    )
)

valid = (
    with_windows
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 07:00:00").cast("timestamp"))
    .select(
        "id",
        "reading_ts",
        "running_temp_c",
        "seq_no",
        "temp_c_avg_6_prior_percent_rank",
        "first_known_corr_status_bits_temp_c",
        "running_corr_status_bits_temp_c",
        "temp_c_avg_6_prior_avg_5_prior",
        F.round("temp_c_avg_6_prior_percent_rank", 1).alias("temp_c_avg_6_prior_percent_rank_rounded"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "running_temp_c",
        "temp_c_avg_6_prior_percent_rank",
        "first_known_corr_status_bits_temp_c",
        "temp_c_avg_6_prior_percent_rank_rounded",
        F.row_number().over(w11).alias("seq_no_2"),
        F.first("temp_c_avg_6_prior_percent_rank", ignorenulls=True).over(w12).alias("first_known_temp_c_avg_6_prior_percent_rank"),
        F.count("seq_no").over(w13).alias("seq_no_count_15m"),
    )
)

result = (
    metrics
    .select(
        "id",
        "first_known_corr_status_bits_temp_c",
        "first_known_temp_c_avg_6_prior_percent_rank",
        "seq_no_count_15m",
    )
    .orderBy(F.col("first_known_temp_c_avg_6_prior_percent_rank").asc_nulls_first())
)
