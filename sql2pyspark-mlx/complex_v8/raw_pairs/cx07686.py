from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_site_id_by_read_at_full = Window.partitionBy("site_id").orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_read_at_week_running = Window.orderBy(F.col("read_at_week").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_read_at_week = Window.orderBy(F.col("read_at_week").desc(), "id")
w_site_id_by_kwh_ties = Window.partitionBy("site_id").orderBy("kwh")
w_n_quality_not_in_est_bad_sit = Window.partitionBy("n_quality_not_in_est_bad", "site_id").orderBy("min_max_kwh_so_far", "site_id")
w_n_quality_not_in_est_bad_sit_2 = Window.partitionBy("n_quality_not_in_est_bad", "site_id").orderBy("min_max_kwh_so_far", "site_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_readings = (
    df_meter_readings
    .filter((F.col("site_id") <= 7) & (F.col("quality").isin("BAD", "OK")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("week", F.col("read_at")).alias("read_at_week"),
    )
)

windowed = (
    filtered_readings
    .select(
        "*",
        F.round(F.avg("voltage").over(w_site_id), 2).alias("site_id_avg_voltage"),
        F.first("read_at_week").over(w_site_id_by_read_at_full).alias("first_read_at_week"),
        F.max("kwh").over(w_all_by_read_at_week_running).alias("max_kwh_so_far"),
        F.min("voltage").over(w_site_id).alias("site_id_min_voltage"),
        F.ntile(10).over(w_all_by_read_at_week).alias("read_at_week_decile"),
        F.size(F.collect_set("quality").over(w_site_id)).alias("distinct_quality_in_site_id"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.dense_rank().over(w_site_id_by_kwh_ties).alias("kwh_dense_rank"),
    )
)

per_group = (
    windowed
    .filter(F.col("site_id") >= 5)
    .groupBy("site_id")
    .agg(
        F.min("max_kwh_so_far").alias("min_max_kwh_so_far"),
        F.count(F.when(~F.col("quality").isin("EST", "BAD"), 1)).alias("n_quality_not_in_est_bad"),
    )
)

with_trends = (
    per_group
    .withColumn("min_max_kwh_so_far_quintile", F.ntile(5).over(w_n_quality_not_in_est_bad_sit))
    .withColumn("last_min_max_kwh_so_far", F.last("min_max_kwh_so_far").over(w_n_quality_not_in_est_bad_sit_2))
)

result = (
    with_trends
    .filter(F.col("last_min_max_kwh_so_far") <= 840.0)
    .select(
        "site_id",
        "min_max_kwh_so_far",
        "n_quality_not_in_est_bad",
        "min_max_kwh_so_far_quintile",
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("n_quality_not_in_est_bad").desc(),
    )
)
