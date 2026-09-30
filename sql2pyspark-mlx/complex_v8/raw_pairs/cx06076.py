from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_by_kwh_per_vol = Window.partitionBy(F.date_trunc("month", "read_at")).orderBy(F.col("kwh_per_voltage").desc_nulls_first())
w_all_by_kwh_per_voltage_full = Window.orderBy(F.col("kwh_per_voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_kwh_per_voltage = Window.orderBy(F.col("kwh_per_voltage").desc(), "id")
w_read_at_to_date = Window.partitionBy(F.to_date("read_at"))
w_read_at_month_by_kwh_per_vol_2 = Window.partitionBy(F.date_trunc("month", "read_at")).orderBy(F.col("kwh_per_voltage").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_read_at_month_by_kwh_per_vol_3 = Window.partitionBy(F.date_trunc("month", "read_at")).orderBy(F.col("kwh_per_voltage").desc_nulls_first(), "id")

cleaned = (
    df_meter_readings
    .filter(F.col("kwh").between(20.3, 87.1))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.round(F.percent_rank().over(w_read_at_month_by_kwh_per_vol), 3).alias("kwh_per_voltage_percent_rank"),
        F.nth_value("read_at", 2).over(w_all_by_kwh_per_voltage_full).alias("second_read_at"),
        F.round(F.cume_dist().over(w_all_by_kwh_per_voltage), 3).alias("kwh_per_voltage_cume_dist"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_read_at_to_date), 2).alias("voltage_vs_read_at_to_date_avg"),
        F.max("voltage").over(w_read_at_month_by_kwh_per_vol_2).alias("voltage_max_3"),
        F.lag("read_at").over(w_read_at_month_by_kwh_per_vol_3).alias("prev_read_at"),
    )
)

joined = (
    with_windows.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select("s.*", F.col("si.commissioned_on").alias("site_commissioned_on"))
)

result = (
    joined
    .cube("quality", "site_id")
    .agg(
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.round(F.sum("voltage_vs_read_at_to_date_avg"), 2).alias("total_voltage_vs_read_at_to_date_avg"),
        F.count("*").alias("n_rows"),
    )
)
