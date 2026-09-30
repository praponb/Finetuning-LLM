from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_no = Window.partitionBy("session_no")
w_all_by_new_session_flag = Window.orderBy("new_session_flag", "id")
w_all_by_new_session_flag_prio = Window.orderBy("new_session_flag", "id").rowsBetween(-3, -1)

recent_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.greatest(F.col("kwh"), F.col("voltage")).alias("greatest_kwh_voltage"),
    )
)

with_prev = recent_readings.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_site_id_running))

ranked = (
    sessionized
    .select(
        "*",
        F.round(F.col("new_session_flag") - F.avg("new_session_flag").over(w_session_no), 2).alias("new_session_flag_vs_session_no_avg"),
        F.count(F.lit(1)).over(w_session_no).alias("rows_in_session_no"),
        F.dense_rank().over(w_all_by_new_session_flag).alias("new_session_flag_dense_rank"),
        F.sum("session_no").over(w_all_by_new_session_flag_prio).alias("session_no_sum_3_prior"),
        F.lead("new_session_flag", 3).over(w_all_by_new_session_flag).alias("next3_new_session_flag"),
        F.size(F.collect_set("site_id").over(w_session_no)).alias("distinct_site_id_in_session_no"),
    )
)

result = (
    ranked
    .cube("quality", "next3_new_session_flag")
    .agg(
        F.countDistinct("site_id").alias("distinct_site_ids"),
        F.sum("new_session_flag_vs_session_no_avg").alias("total_new_session_flag_vs_session_no_avg"),
    )
    .orderBy(
        F.col("quality").asc_nulls_last(),
        F.col("next3_new_session_flag").asc_nulls_last(),
    )
)
