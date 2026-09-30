from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at_ties = Window.orderBy(F.col("read_at").desc())
w_all_by_read_at = Window.orderBy(F.col("read_at").desc(), "id")
w_site_id = Window.partitionBy("site_id")
w_read_at_year = Window.partitionBy(F.year("read_at"))
w_read_at_year_by_voltage_chan = Window.partitionBy(F.year("read_at")).orderBy("voltage_change", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_year_by_voltage_chan_2 = Window.partitionBy(F.year("read_at")).orderBy("voltage_change")
w_read_at_year_by_voltage_chan_3 = Window.partitionBy(F.year("read_at")).orderBy("voltage_change", "id").rowsBetween(-6, Window.currentRow)
w_all_by_voltage_moving_2 = Window.orderBy(F.col("voltage").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_voltage = Window.orderBy(F.col("voltage").desc_nulls_first(), "id")
w_all_by_voltage_prior_6 = Window.orderBy(F.col("voltage").desc_nulls_first(), "id").rowsBetween(-6, -1)
w_all_by_voltage_plus_kwh = Window.orderBy("voltage_plus_kwh", "id")
w_read_at_day_month_by_voltage = Window.partitionBy(F.month("read_at_day")).orderBy("voltage_change", "id")

cleaned_readings = (
    df_meter_readings
    .filter((F.col("quality") != "OK") & (F.to_date(F.col("read_at")) < F.lit("2025-03-08").cast("date")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.date_format(F.col("read_at"), "yyyy-MM").alias("read_at_label"),
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix"),
    )
)

ranked = (
    cleaned_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.dense_rank().over(w_all_by_read_at_ties).alias("read_at_dense_rank"),
        (F.col("voltage") - F.lag("voltage").over(w_all_by_read_at)).alias("voltage_change"),
        F.sum("kwh").over(w_site_id).alias("site_id_sum_kwh"),
        F.min("kwh").over(w_site_id).alias("site_id_min_kwh"),
        F.dense_rank().over(w_all_by_read_at).alias("read_at_dense_rank_2"),
        F.round(F.cume_dist().over(w_all_by_read_at), 3).alias("read_at_cume_dist"),
    )
)

labeled = (
    ranked
    .filter(F.col("site_id_min_kwh").between(360.0, 1020.0))
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "voltage_change",
        "site_id_min_kwh",
        (F.col("voltage") + F.col("kwh")).alias("voltage_plus_kwh"),
    )
)

annotated = (
    labeled
    .select(
        "*",
        F.date_trunc("day", F.col("read_at")).alias("read_at_day"),
        (F.floor(F.col("voltage_plus_kwh") / 50) * 50).alias("voltage_plus_kwh_bucket"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

with_trends = (
    annotated
    .select(
        "id",
        "kwh",
        "voltage",
        "voltage_change",
        "voltage_plus_kwh",
        "read_at_day",
        "voltage_plus_kwh_bucket",
        "days_since_read_at",
        F.max("voltage_change").over(w_read_at_year).alias("read_at_year_max_voltage_change"),
        F.first("site_id_min_kwh").over(w_read_at_year_by_voltage_chan).alias("first_site_id_min_kwh"),
        F.round(F.cume_dist().over(w_read_at_year_by_voltage_chan_2), 3).alias("voltage_change_cume_dist"),
        F.round(F.avg("site_id_min_kwh").over(w_read_at_year_by_voltage_chan_3), 2).alias("site_id_min_kwh_avg_7"),
    )
)

sequenced = (
    with_trends
    .select(
        "*",
        F.round(F.avg("days_since_read_at").over(w_all_by_voltage_moving_2), 2).alias("days_since_read_at_avg_3"),
        F.lead("days_since_read_at").over(w_all_by_voltage).alias("next_days_since_read_at"),
        F.max("site_id_min_kwh_avg_7").over(w_all_by_voltage_prior_6).alias("site_id_min_kwh_avg_7_max_6_prior"),
        F.round(F.cume_dist().over(w_all_by_voltage), 3).alias("voltage_cume_dist"),
        F.round(F.percent_rank().over(w_all_by_voltage_plus_kwh), 3).alias("voltage_plus_kwh_percent_rank"),
    )
)

windowed = (
    sequenced
    .select(
        "*",
        F.ntile(10).over(w_read_at_day_month_by_voltage).alias("voltage_change_decile"),
        F.round(F.cume_dist().over(w_read_at_day_month_by_voltage), 3).alias("voltage_change_cume_dist_2"),
    )
)

result = (
    windowed
    .filter(F.col("voltage_change_cume_dist") > 0.3)
    .select(
        "id",
        "voltage_change",
        "voltage_plus_kwh",
        "read_at_day",
        "voltage_plus_kwh_bucket",
        "read_at_year_max_voltage_change",
        "first_site_id_min_kwh",
        "voltage_change_cume_dist",
        "site_id_min_kwh_avg_7",
        "next_days_since_read_at",
        "site_id_min_kwh_avg_7_max_6_prior",
        "voltage_cume_dist",
        F.quarter(F.col("read_at_day")).alias("read_at_day_quarter"),
    )
    .orderBy(F.col("read_at_day").desc(), F.col("voltage_change").desc())
)
