from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id_by_voltage_p = Window.partitionBy("quality", "site_id").orderBy("voltage_per_kwh", "id")
w_quality_read_at_month_to_dat = Window.partitionBy("quality", F.to_date("read_at_month"))
w_read_at_year_site_id_by_read = Window.partitionBy(F.year("read_at"), "site_id").orderBy(F.col("read_at_month").desc(), "id")
w_quality_by_read_at = Window.partitionBy("quality").orderBy(F.col("read_at").desc(), "id")
w_quality_site_id_by_voltage_p_2 = Window.partitionBy("quality", "site_id").orderBy("voltage_per_kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_readings = (
    df_meter_readings
    .filter(F.col("kwh") >= 26.9)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("month", F.col("read_at")).alias("read_at_month"),
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
    )
)

sequenced = (
    filtered_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "read_at_month",
        "voltage_per_kwh",
        F.row_number().over(w_quality_site_id_by_voltage_p).alias("rn"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_quality_read_at_month_to_dat), F.lit(0)), 4).alias("kwh_share_of_quality_read_at_month_to_date"),
        F.round(F.avg("voltage").over(w_quality_read_at_month_to_dat), 2).alias("quality_read_at_month_to_date_avg_voltage"),
        F.size(F.collect_set("site_id").over(w_quality_read_at_month_to_dat)).alias("distinct_site_id_in_quality_read_at_month_to_dat"),
        F.round(F.cume_dist().over(w_read_at_year_site_id_by_read), 3).alias("read_at_month_cume_dist"),
        F.lead("voltage_per_kwh").over(w_quality_site_id_by_voltage_p).alias("next_voltage_per_kwh"),
        F.round(F.col("voltage_per_kwh") / F.nullif(F.sum("voltage_per_kwh").over(w_quality_read_at_month_to_dat), F.lit(0)), 4).alias("voltage_per_kwh_share_of_quality_read_at_month_t"),
        (F.col("voltage_per_kwh") - F.lag("voltage_per_kwh").over(w_quality_by_read_at)).alias("voltage_per_kwh_change"),
        F.last("read_at_month").over(w_quality_site_id_by_voltage_p_2).alias("last_read_at_month"),
    )
)

annotated = (
    sequenced
    .filter(F.col("quality_read_at_month_to_date_avg_voltage") <= 231.0)
    .select(
        "*",
        F.concat(F.col("quality"), F.lit("#"), F.col("rn").cast("string")).alias("quality_ref"),
        F.greatest(F.col("voltage_per_kwh_change"), F.col("next_voltage_per_kwh")).alias("greatest_voltage_per_kwh_change_next_voltage_per"),
        F.abs(F.col("read_at_month_cume_dist") - 0.7).alias("read_at_month_cume_dist_distance"),
    )
)

result = (
    annotated
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "read_at_month",
        "voltage_per_kwh",
        "kwh_share_of_quality_read_at_month_to_date",
        "distinct_site_id_in_quality_read_at_month_to_dat",
        "voltage_per_kwh_change",
        F.hour(F.col("read_at_month")).alias("read_at_month_hour"),
    )
    .orderBy(F.col("voltage").desc_nulls_first())
)
