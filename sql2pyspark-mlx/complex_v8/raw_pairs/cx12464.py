from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at_prior_3 = Window.orderBy("read_at", "id").rowsBetween(-3, -1)
w_all_by_read_at_quarter = Window.orderBy("read_at_quarter", "id")
w_read_at_year = Window.partitionBy(F.year("read_at"))
w_all_by_read_at = Window.orderBy("read_at", "id")
w_all_by_voltage_moving_6 = Window.orderBy(F.col("voltage").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_voltage_ties = Window.orderBy(F.col("voltage").asc_nulls_last())
w_quality_quality_group = Window.partitionBy("quality", "quality_group")
w_all_by_voltage_running = Window.orderBy(F.col("voltage").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_meter_readings
    .filter(F.col("quality") == "OK")
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("quality").isin("OK", "EST"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
        (F.col("kwh") + F.col("voltage")).alias("kwh_plus_voltage"),
        F.date_trunc("quarter", F.col("read_at")).alias("read_at_quarter"),
    )
)

windowed = (
    cleaned
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "quality_group",
        "kwh_plus_voltage",
        F.round(F.avg("kwh").over(w_all_by_read_at_prior_3), 2).alias("kwh_avg_3_prior"),
        F.rank().over(w_all_by_read_at_quarter).alias("read_at_quarter_rank"),
        F.sum("kwh_plus_voltage").over(w_read_at_year).alias("read_at_year_sum_kwh_plus_voltage"),
        F.max("voltage").over(w_read_at_year).alias("read_at_year_max_voltage"),
        F.ntile(5).over(w_all_by_read_at).alias("read_at_quintile"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_all_by_read_at))) / 60, 1).alias("minutes_since_prev_read_at"),
    )
)

windowed_2 = (
    windowed
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "kwh_avg_3_prior",
        "read_at_year_max_voltage",
        "minutes_since_prev_read_at",
        F.round(F.avg("minutes_since_prev_read_at").over(w_all_by_voltage_moving_6), 2).alias("minutes_since_prev_read_at_avg_7"),
        F.rank().over(w_all_by_voltage_ties).alias("voltage_rank"),
        F.round(F.col("read_at_quintile") - F.avg("read_at_quintile").over(w_quality_quality_group), 2).alias("read_at_quintile_vs_quality_quality_group_avg"),
        F.max("voltage").over(w_all_by_voltage_running).alias("max_voltage_so_far"),
    )
)

result = (
    windowed_2
    .filter(F.col("read_at_year_max_voltage") > 994.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "kwh_avg_3_prior",
        "voltage_rank",
        "read_at_quintile_vs_quality_quality_group_avg",
        "max_voltage_so_far",
        F.date_trunc("week", F.col("read_at")).alias("read_at_week"),
    )
    .orderBy(F.col("max_voltage_so_far").desc_nulls_first(), "id")
    .limit(10)
)
