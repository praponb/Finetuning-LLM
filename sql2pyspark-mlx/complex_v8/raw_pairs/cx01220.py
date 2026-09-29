from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_kwh = Window.partitionBy("quality").orderBy(F.col("kwh").desc(), "id")
w_quality = Window.partitionBy("quality")
w_read_at_to_date = Window.partitionBy(F.to_date("read_at"))
w_quality_by_voltage = Window.partitionBy("quality").orderBy(F.col("voltage").desc(), "id")
w_quality_by_kwh_running = Window.partitionBy("quality").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_kwh_quartile = Window.partitionBy("site_id", "kwh_quartile")
w_first_known_read_at_month = Window.partitionBy(F.date_trunc("month", "first_known_read_at"))
w_site_id_quality_by_next_volt = Window.partitionBy("site_id", "quality").orderBy(F.col("next_voltage").desc())
w_all_by_quality_max_kwh = Window.orderBy(F.col("quality_max_kwh").desc_nulls_first(), "id")

base = (
    df_meter_readings
    .filter(F.col("voltage") > 215.8)
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
    base
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.ntile(4).over(w_quality_by_kwh).alias("kwh_quartile"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
        F.max("kwh").over(w_quality).alias("quality_max_kwh"),
        F.min("voltage").over(w_read_at_to_date).alias("read_at_to_date_min_voltage"),
        F.sum("kwh").over(w_read_at_to_date).alias("read_at_to_date_sum_kwh"),
        F.lead("voltage").over(w_quality_by_voltage).alias("next_voltage"),
        F.round(F.avg("voltage").over(w_read_at_to_date), 2).alias("read_at_to_date_avg_voltage"),
        F.first("read_at", ignorenulls=True).over(w_quality_by_kwh_running).alias("first_known_read_at"),
    )
)

scored = (
    windowed
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "kwh_quartile",
        "rows_in_quality",
        "quality_max_kwh",
        "next_voltage",
        "read_at_to_date_avg_voltage",
        "first_known_read_at",
        F.concat(F.col("quality"), F.lit("#"), F.col("kwh_quartile").cast("string")).alias("quality_ref"),
    )
)

windowed_2 = (
    scored
    .select(
        "*",
        F.round(F.col("rows_in_quality") - F.avg("rows_in_quality").over(w_site_id_kwh_quartile), 2).alias("rows_in_quality_vs_site_id_kwh_quartile_avg"),
        F.size(F.collect_set("quality").over(w_first_known_read_at_month)).alias("distinct_quality_in_first_known_read_at_month"),
        F.rank().over(w_site_id_quality_by_next_volt).alias("next_voltage_rank"),
        F.rank().over(w_all_by_quality_max_kwh).alias("quality_max_kwh_rank"),
        F.round(F.col("next_voltage") / F.nullif(F.sum("next_voltage").over(w_site_id_kwh_quartile), F.lit(0)), 4).alias("next_voltage_share_of_site_id_kwh_quartile"),
        F.count(F.lit(1)).over(w_first_known_read_at_month).alias("rows_in_first_known_read_at_month"),
    )
)

result = (
    windowed_2
    .select(
        "id",
        "read_at",
        "kwh",
        "quality",
        "kwh_quartile",
        "rows_in_quality",
        "quality_max_kwh",
        "next_voltage",
        "read_at_to_date_avg_voltage",
        "first_known_read_at",
        "quality_ref",
        "rows_in_quality_vs_site_id_kwh_quartile_avg",
        "distinct_quality_in_first_known_read_at_month",
        "next_voltage_rank",
        "quality_max_kwh_rank",
    )
    .orderBy(F.col("quality").desc(), F.col("id").desc())
)
