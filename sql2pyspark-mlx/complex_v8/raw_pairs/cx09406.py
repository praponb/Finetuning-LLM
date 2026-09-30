from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id = Window.partitionBy("quality", "site_id")
w_all_by_kwh = Window.orderBy(F.col("kwh").desc(), "id")
w_all_by_kwh_running = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_kwh_moving_3 = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_kwh_ties = Window.orderBy(F.col("kwh").desc())
w_read_at_month_quality = Window.partitionBy(F.month("read_at"), "quality")
w_site_id_by_voltage_vs_qualit = Window.partitionBy("site_id").orderBy(F.col("voltage_vs_quality_site_id_avg").desc(), "id").rowsBetween(-3, -1)
w_site_id_by_voltage_vs_qualit_2 = Window.partitionBy("site_id").orderBy(F.col("voltage_vs_quality_site_id_avg").desc(), "id")
w_kwh_rank_by_kwh_rank = Window.partitionBy("kwh_rank").orderBy("kwh_rank", "id")

prepared = (
    df_meter_readings
    .filter((F.col("quality") != "EST") & (F.col("kwh") > 26.9))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

windowed = (
    prepared
    .select(
        "*",
        F.count(F.lit(1)).over(w_quality_site_id).alias("rows_in_quality_site_id"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality_site_id), 2).alias("voltage_vs_quality_site_id_avg"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_quality_site_id), F.lit(0)), 4).alias("kwh_share_of_quality_site_id"),
        F.ntile(3).over(w_all_by_kwh).alias("kwh_tercile"),
        F.row_number().over(w_all_by_kwh).alias("seq_no"),
        F.round(F.avg("voltage").over(w_quality_site_id), 2).alias("quality_site_id_avg_voltage"),
        F.sum("kwh").over(w_all_by_kwh_running).alias("running_kwh"),
        F.max("kwh").over(w_quality_site_id).alias("quality_site_id_max_kwh"),
        F.max("kwh").over(w_all_by_kwh_moving_3).alias("kwh_max_4"),
        F.last("read_at", ignorenulls=True).over(w_all_by_kwh_running).alias("last_known_read_at"),
        F.round(F.cume_dist().over(w_all_by_kwh), 3).alias("kwh_cume_dist"),
        F.rank().over(w_all_by_kwh_ties).alias("kwh_rank"),
    )
)

top_rows = (
    windowed
    .filter(F.col("seq_no") == 1)
    .drop("seq_no")
)

ranked = (
    top_rows
    .select(
        "*",
        F.max("kwh_max_4").over(w_read_at_month_quality).alias("read_at_month_quality_max_kwh_max_4"),
        F.round(F.avg("kwh_max_4").over(w_site_id_by_voltage_vs_qualit), 2).alias("kwh_max_4_avg_3_prior"),
        F.round(F.avg("rows_in_quality_site_id").over(w_read_at_month_quality), 2).alias("read_at_month_quality_avg_rows_in_quality_site_i"),
        F.lead("voltage").over(w_site_id_by_voltage_vs_qualit_2).alias("next_voltage"),
        (F.col("quality_site_id_avg_voltage") - F.lag("quality_site_id_avg_voltage").over(w_kwh_rank_by_kwh_rank)).alias("quality_site_id_avg_voltage_change"),
    )
)

result = (
    ranked
    .filter(F.col("read_at_month_quality_max_kwh_max_4") >= 1260.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "voltage_vs_quality_site_id_avg",
        "kwh_share_of_quality_site_id",
        "kwh_tercile",
        "quality_site_id_avg_voltage",
        "quality_site_id_max_kwh",
        "kwh_max_4",
        "last_known_read_at",
        "kwh_cume_dist",
        "kwh_rank",
        "read_at_month_quality_max_kwh_max_4",
        "kwh_max_4_avg_3_prior",
        "read_at_month_quality_avg_rows_in_quality_site_i",
        "next_voltage",
        "quality_site_id_avg_voltage_change",
    )
    .orderBy(
        F.col("kwh_cume_dist").desc(),
        F.col("quality_site_id_avg_voltage").asc(),
    )
)
