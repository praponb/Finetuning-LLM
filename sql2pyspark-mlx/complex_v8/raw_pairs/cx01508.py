from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_total_is_new_session = Window.partitionBy("quality", "total_is_new_session").orderBy(F.col("avg_session_id").asc_nulls_last())
w_quality_total_is_new_session_2 = Window.partitionBy("quality", "total_is_new_session").orderBy(F.col("avg_session_id").asc_nulls_last(), "voltage_tier", "quality", "read_at_year").rowsBetween(-6, -1)
w_read_at_year_total_is_new_se = Window.partitionBy("read_at_year", "total_is_new_session")
w_total_is_new_session_voltage = Window.partitionBy("total_is_new_session", "voltage_tier").orderBy("avg_kwh", "voltage_tier", "quality", "read_at_year")
w_total_is_new_session_voltage_2 = Window.partitionBy("total_is_new_session", "voltage_tier").orderBy("avg_kwh", "voltage_tier", "quality", "read_at_year").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("voltage") >= 238.8, "HIGH").when(F.col("voltage") >= 222.3, "MEDIUM").otherwise("LOW").alias("voltage_tier"),
    )
)

with_prev = base.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_site_id_running))

per_group = (
    sessionized
    .groupBy("voltage_tier", "quality", F.year("read_at").alias("read_at_year"))
    .agg(
        F.sum("is_new_session").alias("total_is_new_session"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.round(F.avg("session_id"), 2).alias("avg_session_id"),
        F.round(F.avg("kwh"), 2).alias("avg_kwh"),
    )
)

ranked = (
    per_group
    .select(
        "voltage_tier",
        "quality",
        "read_at_year",
        "total_is_new_session",
        "quality_list",
        "avg_kwh",
        F.round(F.cume_dist().over(w_quality_total_is_new_session), 3).alias("avg_session_id_cume_dist"),
        F.round(F.avg("avg_session_id").over(w_quality_total_is_new_session_2), 2).alias("avg_session_id_avg_6_prior"),
        F.round(F.col("avg_session_id") - F.avg("avg_session_id").over(w_read_at_year_total_is_new_se), 2).alias("avg_session_id_vs_read_at_year_total_is_new_sess"),
        F.size(F.collect_set("quality").over(w_read_at_year_total_is_new_se)).alias("distinct_quality_in_read_at_year_total_is_new_se"),
        (F.col("avg_session_id") - F.lag("avg_session_id").over(w_total_is_new_session_voltage)).alias("avg_session_id_change"),
        F.sum("avg_kwh").over(w_total_is_new_session_voltage_2).alias("running_avg_kwh"),
    )
)

result = (
    ranked
    .filter(F.col("quality") == "OK")
    .select(
        "voltage_tier",
        "read_at_year",
        "total_is_new_session",
        "avg_kwh",
        "avg_session_id_cume_dist",
        "avg_session_id_avg_6_prior",
        "avg_session_id_vs_read_at_year_total_is_new_sess",
        "distinct_quality_in_read_at_year_total_is_new_se",
        "avg_session_id_change",
        "running_avg_kwh",
        F.round(F.col("running_avg_kwh") * 0.9, 2).alias("running_avg_kwh_adj"),
    )
)
