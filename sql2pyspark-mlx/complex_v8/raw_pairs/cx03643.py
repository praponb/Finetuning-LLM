from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id_by_read_at_y = Window.partitionBy("quality", "site_id").orderBy(F.col("read_at_year").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_site_id_by_read_at = Window.partitionBy("quality", "site_id").orderBy("read_at", "id")
w_quality_site_id_by_read_at_y_2 = Window.partitionBy("quality", "site_id").orderBy(F.col("read_at_year").asc_nulls_last(), "id")
w_site_id_read_at_to_date = Window.partitionBy("site_id", F.to_date("read_at"))
w_site_id_by_kwh_minus_voltage = Window.partitionBy("site_id").orderBy(F.col("kwh_minus_voltage").desc(), "id")
w_quality_site_id_by_read_at_t = Window.partitionBy("quality", "site_id").orderBy("read_at")
w_quality_site_id_by_read_at_y_3 = Window.partitionBy("quality", "site_id").orderBy(F.col("read_at_year").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_quality_site_id_by_read_at_f = Window.partitionBy("quality", "site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_site_id_by_read_at_r = Window.partitionBy("quality", "site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality = Window.partitionBy("quality")
w_all_by_site_id_read_at_to_da = Window.orderBy("site_id_read_at_to_date_max_kwh_minus_voltage", "id")
w_read_at_year_quartile_site_i = Window.partitionBy("read_at_year_quartile", "site_id").orderBy("kwh_avg_4", "id")
w_read_at_year_quartile_site_i_2 = Window.partitionBy("read_at_year_quartile", "site_id").orderBy("kwh_avg_4", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_readings = (
    df_meter_readings
    .filter((F.col("quality").isNotNull()) & (F.col("read_at") < F.lit("2025-03-08 12:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("year", F.col("read_at")).alias("read_at_year"),
        (F.col("kwh") - F.col("voltage")).alias("kwh_minus_voltage"),
    )
)

with_trends = (
    valid_readings
    .select(
        "*",
        F.nth_value("read_at_year", 2).over(w_quality_site_id_by_read_at_y).alias("second_read_at_year"),
        F.dense_rank().over(w_quality_site_id_by_read_at).alias("read_at_dense_rank"),
        F.ntile(4).over(w_quality_site_id_by_read_at_y_2).alias("read_at_year_quartile"),
        F.max("kwh_minus_voltage").over(w_site_id_read_at_to_date).alias("site_id_read_at_to_date_max_kwh_minus_voltage"),
        F.row_number().over(w_site_id_by_kwh_minus_voltage).alias("seq_no"),
        F.round(F.percent_rank().over(w_quality_site_id_by_read_at_t), 3).alias("read_at_percent_rank"),
        F.round(F.avg("kwh").over(w_quality_site_id_by_read_at_y_3), 2).alias("kwh_avg_4"),
        F.nth_value("voltage", 2).over(w_quality_site_id_by_read_at_f).alias("second_voltage"),
        F.count(F.lit(1)).over(w_quality_site_id_by_read_at_r).alias("running_count"),
    )
)

leaders = (
    with_trends
    .filter((F.col("seq_no") == 1) & (F.col("site_id_read_at_to_date_max_kwh_minus_voltage").between(547.5, 3285.0)))
    .drop("seq_no")
)

ranked = (
    leaders
    .select(
        "*",
        F.round(F.col("read_at_dense_rank") / F.nullif(F.sum("read_at_dense_rank").over(w_quality), F.lit(0)), 4).alias("read_at_dense_rank_share_of_quality"),
        F.ntile(5).over(w_all_by_site_id_read_at_to_da).alias("site_id_read_at_to_date_max_kwh_minus_voltage_qu"),
        F.rank().over(w_read_at_year_quartile_site_i).alias("kwh_avg_4_rank"),
        F.last("read_at_year").over(w_read_at_year_quartile_site_i_2).alias("last_read_at_year"),
        F.max("read_at_percent_rank").over(w_quality).alias("quality_max_read_at_percent_rank"),
    )
)

result = (
    ranked
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "read_at_year",
        "second_read_at_year",
        "read_at_dense_rank",
        "read_at_percent_rank",
        "kwh_avg_4",
        "read_at_dense_rank_share_of_quality",
        "site_id_read_at_to_date_max_kwh_minus_voltage_qu",
        "last_read_at_year",
        F.round(F.col("read_at_year_quartile") / F.nullif(F.col("kwh_avg_4_rank"), F.lit(0)), 3).alias("read_at_year_quartile_per_kwh_avg_4_rank"),
    )
    .orderBy("second_read_at_year")
)
