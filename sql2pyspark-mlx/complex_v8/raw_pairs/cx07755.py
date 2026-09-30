from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_voltage_running = Window.orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_voltage_full = Window.partitionBy("site_id").orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_voltage_prior_3 = Window.orderBy(F.col("voltage").desc(), "id").rowsBetween(-3, -1)
w_quality_site_id_by_voltage = Window.partitionBy("quality", "site_id").orderBy("voltage", "id")
w_kwh_tier = Window.partitionBy("kwh_tier")
w_all_by_voltage_running_2 = Window.orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_voltage = Window.orderBy("voltage", "id")
w_all_by_voltage_moving_1 = Window.orderBy("voltage", "id").rowsBetween(-1, Window.currentRow)
w_all_by_voltage_2 = Window.orderBy(F.col("voltage").desc(), "id")
w_site_id_by_voltage = Window.partitionBy("site_id").orderBy(F.col("voltage").desc(), "id")
w_quality_site_id_by_voltage_f = Window.partitionBy("quality", "site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("kwh") >= 109.0, "A").when(F.col("kwh") >= 33.9, "B").otherwise("C").alias("kwh_tier"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "quality",
        F.last("voltage", ignorenulls=True).over(w_all_by_voltage_running).alias("last_known_voltage"),
        F.last("kwh").over(w_site_id_by_voltage_full).alias("last_kwh"),
        F.round(F.avg("kwh").over(w_all_by_voltage_prior_3), 2).alias("kwh_avg_3_prior"),
        F.rank().over(w_quality_site_id_by_voltage).alias("voltage_rank"),
        F.min("voltage").over(w_kwh_tier).alias("kwh_tier_min_voltage"),
        F.last("kwh", ignorenulls=True).over(w_all_by_voltage_running_2).alias("last_known_kwh"),
        (F.col("voltage") - F.lag("voltage").over(w_all_by_voltage)).alias("voltage_change"),
        F.sum("kwh").over(w_all_by_voltage_running_2).alias("running_kwh"),
        F.ntile(10).over(w_all_by_voltage).alias("voltage_decile"),
        F.max("voltage").over(w_all_by_voltage_moving_1).alias("voltage_max_2"),
        F.round(F.percent_rank().over(w_quality_site_id_by_voltage), 3).alias("voltage_percent_rank"),
        F.lead("kwh", 2, 0).over(w_all_by_voltage_2).alias("next2_kwh"),
        (F.col("voltage") - F.lag("voltage").over(w_quality_site_id_by_voltage)).alias("voltage_change_2"),
        F.rank().over(w_site_id_by_voltage).alias("voltage_rank_2"),
        F.max("voltage").over(w_kwh_tier).alias("kwh_tier_max_voltage"),
        F.nth_value("voltage", 2).over(w_quality_site_id_by_voltage_f).alias("second_voltage"),
    )
)

annotated = (
    with_windows
    .select(
        "*",
        F.abs(F.col("running_kwh") - 600.0).alias("running_kwh_distance"),
        F.when(F.col("voltage_change_2") >= 73.5, "A").when(F.col("voltage_change_2") >= -122.5, "B").otherwise("C").alias("voltage_change_2_tier"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

per_group = (
    annotated
    .groupBy("voltage_rank_2")
    .agg(
        F.countDistinct("quality").alias("distinct_qualitys"),
        F.sum(F.when(F.col("quality").isin("EST", "OK"), 1).otherwise(0)).alias("n_quality_in_est_ok"),
        F.count("*").alias("n_rows"),
    )
)

result = (
    per_group
    .filter(F.col("n_rows") > 2)
    .select(
        "voltage_rank_2",
        "distinct_qualitys",
        "n_quality_in_est_ok",
        "n_rows",
        F.greatest(F.col("n_rows"), F.col("n_quality_in_est_ok")).alias("greatest_n_rows_n_quality_in_est_ok"),
        F.abs(F.col("distinct_qualitys") - 4).alias("distinct_qualitys_distance"),
    )
    .orderBy(
        F.col("greatest_n_rows_n_quality_in_est_ok").asc(),
        F.col("voltage_rank_2").desc_nulls_first(),
    )
)
