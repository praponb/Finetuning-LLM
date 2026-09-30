from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_all_by_voltage = Window.orderBy(F.col("voltage").desc_nulls_first(), "id")
w_all_by_voltage_ties = Window.orderBy("voltage")
w_all_by_voltage_2 = Window.orderBy("voltage", "id")
w_site_id_read_at_year = Window.partitionBy("site_id", F.year("read_at"))
w_site_id_by_kwh_full = Window.partitionBy("site_id").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_plus_7d_month_site_i = Window.partitionBy(F.month("read_at_plus_7d"), "site_id").orderBy(F.col("kwh_change").desc(), "id")
w_all_by_voltage_dense_rank_ti = Window.orderBy("voltage_dense_rank")
w_read_at_month_by_site_id_max = Window.partitionBy(F.date_trunc("month", "read_at")).orderBy(F.col("site_id_max_kwh").asc_nulls_last(), "id")
w_all_by_voltage_dense_rank_pr = Window.orderBy("voltage_dense_rank", "id").rowsBetween(-2, -1)

scoped_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
    )
)

metrics = (
    scoped_readings
    .select(
        "*",
        F.round(F.avg("voltage").over(w_site_id), 2).alias("site_id_avg_voltage"),
        F.dense_rank().over(w_all_by_voltage).alias("voltage_dense_rank"),
        F.round(F.percent_rank().over(w_all_by_voltage_ties), 3).alias("voltage_percent_rank"),
        (F.col("kwh") - F.lag("kwh").over(w_all_by_voltage)).alias("kwh_change"),
        F.lead("kwh", 3, 0).over(w_all_by_voltage).alias("next3_kwh"),
        F.dense_rank().over(w_all_by_voltage_2).alias("voltage_dense_rank_2"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id_read_at_year), F.lit(0)), 4).alias("kwh_share_of_site_id_read_at_year"),
        F.rank().over(w_all_by_voltage).alias("voltage_rank"),
        F.nth_value("voltage", 2).over(w_site_id_by_kwh_full).alias("second_voltage"),
        F.round(F.cume_dist().over(w_all_by_voltage_ties), 3).alias("voltage_cume_dist"),
        F.max("kwh").over(w_site_id).alias("site_id_max_kwh"),
    )
)

enriched = metrics.withColumn("read_at_plus_7d", F.col("read_at") + F.expr("INTERVAL 7 DAYS"))

metrics_2 = (
    enriched
    .select(
        "*",
        (F.col("second_voltage") - F.lag("second_voltage").over(w_read_at_plus_7d_month_site_i)).alias("second_voltage_change"),
        F.round(F.percent_rank().over(w_all_by_voltage_dense_rank_ti), 3).alias("voltage_dense_rank_percent_rank"),
        F.rank().over(w_read_at_month_by_site_id_max).alias("site_id_max_kwh_rank"),
        F.round(F.avg("kwh").over(w_all_by_voltage_dense_rank_pr), 2).alias("kwh_avg_2_prior"),
        F.row_number().over(w_read_at_month_by_site_id_max).alias("site_id_max_kwh_seq"),
    )
)

result = (
    metrics_2
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "site_id_avg_voltage",
        "voltage_dense_rank",
        "next3_kwh",
        "rows_in_site_id",
        "kwh_share_of_site_id_read_at_year",
        "voltage_rank",
        "second_voltage",
        "site_id_max_kwh",
        "second_voltage_change",
        "voltage_dense_rank_percent_rank",
        "site_id_max_kwh_rank",
        F.greatest(F.col("voltage_dense_rank"), F.col("second_voltage")).alias("greatest_voltage_dense_rank_second_voltage"),
    )
    .orderBy(F.col("site_id_avg_voltage").desc(), "voltage")
)
