from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id = Window.partitionBy("quality", "site_id")
w_read_at_year_site_id_by_volt = Window.partitionBy(F.year("read_at"), "site_id").orderBy(F.col("voltage").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_read_at_year_site_id_by_volt_2 = Window.partitionBy(F.year("read_at"), "site_id").orderBy(F.col("voltage").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_read_at_year_site_id_by_volt_3 = Window.partitionBy(F.year("read_at"), "site_id").orderBy(F.col("voltage").asc_nulls_last(), "id")
w_all_by_voltage_ties = Window.orderBy("voltage")
w_all_by_voltage_running = Window.orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_voltage = Window.orderBy("voltage", "id")
w_quality_by_read_at = Window.partitionBy("quality").orderBy(F.col("read_at").desc(), "id")

scoped_readings = (
    df_meter_readings
    .filter(F.col("voltage") >= 214.3)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

ranked = (
    scoped_readings
    .select(
        "*",
        F.round(F.avg("voltage").over(w_read_at_year_site_id_by_volt), 2).alias("voltage_avg_2_prior"),
        F.count(F.lit(1)).over(w_quality_site_id).alias("rows_in_quality_site_id"),
        F.sum("voltage").over(w_read_at_year_site_id_by_volt_2).alias("voltage_sum_3_prior"),
        F.ntile(4).over(w_read_at_year_site_id_by_volt_3).alias("voltage_quartile"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality_site_id), 2).alias("voltage_vs_quality_site_id_avg"),
        F.rank().over(w_all_by_voltage_ties).alias("voltage_rank"),
        F.last("voltage", ignorenulls=True).over(w_all_by_voltage_running).alias("last_known_voltage"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_all_by_voltage))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_quality_by_read_at))) / 60, 1).alias("minutes_since_prev_read_at_2"),
        F.sum("voltage").over(w_quality_site_id).alias("quality_site_id_sum_voltage"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_read_at_year_site_id_by_volt_3))) / 60, 1).alias("minutes_since_prev_read_at_3"),
    )
)

leaders = (
    ranked
    .filter(F.col("voltage_rank") <= 5)
    .drop("voltage_rank")
)

result = (
    leaders
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "quality_slug",
        "rows_in_quality_site_id",
        "voltage_quartile",
        "voltage_vs_quality_site_id_avg",
        "last_known_voltage",
        "minutes_since_prev_read_at",
        "quality_site_id_sum_voltage",
        "minutes_since_prev_read_at_3",
        F.round(F.col("voltage") * 0.9, 2).alias("voltage_adj"),
        F.concat(F.col("quality_slug"), F.lit("#"), F.col("rows_in_quality_site_id").cast("string")).alias("quality_slug_ref"),
    )
    .orderBy(F.col("id").asc(), F.col("minutes_since_prev_read_at_3").desc())
)
