from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id = Window.partitionBy("quality", "site_id")
w_all_by_least_voltage_kwh_ful = Window.orderBy(F.col("least_voltage_kwh").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_tariff_quality_by_read = Window.partitionBy("site_tariff", "quality").orderBy("read_at", "id")
w_quality_by_least_voltage_kwh = Window.partitionBy("quality").orderBy("least_voltage_kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_least_voltage_kwh_pri = Window.orderBy(F.col("least_voltage_kwh").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_site_id = Window.partitionBy("site_id")
w_quality_by_least_voltage_kwh_2 = Window.partitionBy("quality").orderBy("least_voltage_kwh", "id").rowsBetween(-2, -1)
w_site_id_site_tariff_by_read = Window.partitionBy("site_id", "site_tariff").orderBy("read_at", "id")

prepared = (
    df_meter_readings
    .filter(F.hour(F.col("read_at")).between(10, 14))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.least(F.col("voltage"), F.col("kwh")).alias("least_voltage_kwh"),
    )
)

combined_details_site = (
    prepared.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select("s.*", F.col("si.tariff").alias("site_tariff"))
)

windowed = (
    combined_details_site
    .select(
        "*",
        F.round(F.col("least_voltage_kwh") - F.avg("least_voltage_kwh").over(w_quality_site_id), 2).alias("least_voltage_kwh_vs_quality_site_id_avg"),
        F.first("read_at").over(w_all_by_least_voltage_kwh_ful).alias("first_read_at"),
        F.row_number().over(w_site_tariff_quality_by_read).alias("rn"),
        F.sum("kwh").over(w_quality_site_id).alias("quality_site_id_sum_kwh"),
        F.sum("kwh").over(w_quality_by_least_voltage_kwh).alias("running_kwh"),
        F.rank().over(w_site_tariff_quality_by_read).alias("read_at_rank"),
        F.round(F.avg("least_voltage_kwh").over(w_all_by_least_voltage_kwh_pri), 2).alias("least_voltage_kwh_avg_2_prior"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.round(F.col("least_voltage_kwh") / F.nullif(F.sum("least_voltage_kwh").over(w_site_id), F.lit(0)), 4).alias("least_voltage_kwh_share_of_site_id"),
        F.round(F.avg("least_voltage_kwh").over(w_quality_by_least_voltage_kwh_2), 2).alias("least_voltage_kwh_avg_2_prior_2"),
        F.first("kwh", ignorenulls=True).over(w_quality_by_least_voltage_kwh).alias("first_known_kwh"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id), 2).alias("kwh_vs_site_id_avg"),
        (F.col("least_voltage_kwh") - F.lag("least_voltage_kwh").over(w_site_id_site_tariff_by_read)).alias("least_voltage_kwh_change"),
    )
)

first_rows = (
    windowed
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    first_rows
    .select(
        "id",
        "site_id",
        "least_voltage_kwh",
        "site_tariff",
        "least_voltage_kwh_vs_quality_site_id_avg",
        "first_read_at",
        "quality_site_id_sum_kwh",
        "running_kwh",
        "read_at_rank",
        "least_voltage_kwh_avg_2_prior_2",
        "first_known_kwh",
        "kwh_vs_site_id_avg",
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
    )
    .orderBy(
        F.col("quality_site_id_sum_kwh").asc(),
        F.col("first_known_kwh").desc(),
        "id",
    )
    .limit(10)
)
