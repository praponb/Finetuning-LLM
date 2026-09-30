from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_end_year = Window.partitionBy(F.year("session_end"))
w_all_by_duration_min_prior_1 = Window.orderBy(F.col("duration_min").desc(), "site_id", "session_no").rowsBetween(-1, -1)

cleaned = df_meter_readings

with_prev = cleaned.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_site_id_running))

session_summary = (
    sessionized
    .groupBy("site_id", "session_no")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("kwh").alias("session_kwh"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

sequenced = (
    session_summary
    .select(
        "site_id",
        "session_no",
        "session_start",
        "session_end",
        "events",
        "session_kwh",
        F.count(F.lit(1)).over(w_session_end_year).alias("rows_in_session_end_year"),
        F.sum("session_no").over(w_all_by_duration_min_prior_1).alias("session_no_sum_1_prior"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["session_no", "site_id"], ["session_no"], []],
        "session_no", "site_id",
    )
    .agg(
        F.grouping("session_no").alias("is_total_session_no"),
        F.max("rows_in_session_end_year").alias("max_rows_in_session_end_year"),
        F.count("*").alias("n_rows"),
    )
)
