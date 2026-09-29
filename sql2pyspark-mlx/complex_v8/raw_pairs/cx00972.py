from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_read_at_moving_1 = Window.partitionBy("quality").orderBy(F.col("read_at").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_quality_by_read_at_running = Window.partitionBy("quality").orderBy(F.col("read_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_to_date = Window.partitionBy(F.to_date("read_at"))
w_quality_by_read_at_moving_6 = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(-6, Window.currentRow)
w_quality_by_read_at = Window.partitionBy("quality").orderBy(F.col("read_at").asc_nulls_last(), "id")
w_all_by_read_at = Window.orderBy(F.col("read_at").desc(), "id")
w_distinct_site_id_in_read_at = Window.partitionBy("distinct_site_id_in_read_at_to_date")
w_site_id_quality_by_read_at = Window.partitionBy("site_id", "quality").orderBy(F.col("read_at").asc_nulls_last(), "id")
w_first_known_read_at_to_date = Window.partitionBy(F.to_date("first_known_read_at")).orderBy("first_known_read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_readings = (
    df_meter_readings
    .filter((F.col("quality").isin("BAD", "EST")) & (F.col("site_id") <= 8))
)

ranked = (
    scoped_readings
    .select(
        "*",
        F.round(F.avg("kwh").over(w_quality_by_read_at_moving_1), 2).alias("kwh_avg_2"),
        F.first("read_at", ignorenulls=True).over(w_quality_by_read_at_running).alias("first_known_read_at"),
        F.round(F.avg("kwh").over(w_read_at_to_date), 2).alias("read_at_to_date_avg_kwh"),
        F.size(F.collect_set("site_id").over(w_read_at_to_date)).alias("distinct_site_id_in_read_at_to_date"),
        F.sum("voltage").over(w_quality_by_read_at_moving_6).alias("voltage_sum_7"),
        F.dense_rank().over(w_quality_by_read_at).alias("read_at_dense_rank"),
        F.min("voltage").over(w_quality_by_read_at_running).alias("min_voltage_so_far"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_read_at_to_date), F.lit(0)), 4).alias("kwh_share_of_read_at_to_date"),
        F.round(F.percent_rank().over(w_all_by_read_at), 3).alias("read_at_percent_rank"),
        F.lead("voltage", 1, 0).over(w_quality_by_read_at).alias("next_voltage"),
        F.sum("voltage").over(w_read_at_to_date).alias("read_at_to_date_sum_voltage"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_read_at_to_date), 2).alias("kwh_vs_read_at_to_date_avg"),
    )
)

top_rows = (
    ranked
    .filter((F.col("read_at_dense_rank") <= 3) & (F.col("kwh").between(43.1, 117.4)))
    .drop("read_at_dense_rank")
)

with_windows = (
    top_rows
    .select(
        "*",
        F.min("read_at_to_date_avg_kwh").over(w_read_at_to_date).alias("read_at_to_date_min_read_at_to_date_avg_kwh"),
        F.max("kwh").over(w_distinct_site_id_in_read_at).alias("distinct_site_id_in_read_at_to_date_max_kwh"),
        F.lead("kwh").over(w_site_id_quality_by_read_at).alias("next_kwh"),
        F.round(F.col("kwh_avg_2") / F.nullif(F.sum("kwh_avg_2").over(w_distinct_site_id_in_read_at), F.lit(0)), 4).alias("kwh_avg_2_share_of_distinct_site_id_in_read_at_t"),
        F.round(F.col("kwh_avg_2") - F.avg("kwh_avg_2").over(w_read_at_to_date), 2).alias("kwh_avg_2_vs_read_at_to_date_avg"),
        F.last("read_at_to_date_avg_kwh", ignorenulls=True).over(w_first_known_read_at_to_date).alias("last_known_read_at_to_date_avg_kwh"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "first_known_read_at",
        "read_at_to_date_avg_kwh",
        "distinct_site_id_in_read_at_to_date",
        "voltage_sum_7",
        "read_at_percent_rank",
        "next_voltage",
        "kwh_vs_read_at_to_date_avg",
        "read_at_to_date_min_read_at_to_date_avg_kwh",
        "distinct_site_id_in_read_at_to_date_max_kwh",
        "next_kwh",
        "kwh_avg_2_share_of_distinct_site_id_in_read_at_t",
        "kwh_avg_2_vs_read_at_to_date_avg",
        (F.col("kwh_avg_2") + F.col("voltage")).alias("kwh_avg_2_plus_voltage"),
    )
    .orderBy("distinct_site_id_in_read_at_to_date")
)
