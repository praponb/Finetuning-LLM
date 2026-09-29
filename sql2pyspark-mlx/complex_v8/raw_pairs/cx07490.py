from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_kwh_moving_3 = Window.partitionBy("site_id").orderBy(F.col("kwh").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_kwh_full = Window.orderBy(F.col("kwh").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_read_at = Window.orderBy("read_at", "id")
w_quality = Window.partitionBy("quality")
w_all_by_read_at_full = Window.orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_read_at_prior_3 = Window.orderBy("read_at", "id").rowsBetween(-3, -1)

valid = (
    df_meter_readings
    .filter(F.col("site_id") >= 5)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.least(F.col("kwh"), F.col("voltage")).alias("least_kwh_voltage"),
    )
)

joined = (
    valid.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.zone").isin("Z2", "Z4")), "inner")
    .filter(F.col("si.installed_kw") >= 96.9)
    .select("s.*", F.col("si.installed_kw").alias("site_installed_kw"))
)

windowed = (
    joined
    .select(
        "*",
        F.max("kwh").over(w_site_id_by_kwh_moving_3).alias("kwh_max_4"),
        F.nth_value("read_at", 2).over(w_all_by_kwh_full).alias("second_read_at"),
        F.dense_rank().over(w_all_by_read_at).alias("read_at_dense_rank"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
        F.nth_value("read_at", 2).over(w_all_by_read_at_full).alias("second_read_at_2"),
        F.max("site_installed_kw").over(w_all_by_read_at_prior_3).alias("site_installed_kw_max_3_prior"),
        F.size(F.collect_set("site_id").over(w_quality)).alias("distinct_site_id_in_quality"),
    )
)

result = (
    windowed
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "least_kwh_voltage",
        "site_installed_kw",
        "kwh_max_4",
        "second_read_at",
        "read_at_dense_rank",
        "rows_in_quality",
        "second_read_at_2",
        "site_installed_kw_max_3_prior",
        (F.col("rows_in_quality") - F.col("kwh")).alias("rows_in_quality_minus_kwh"),
    )
    .orderBy(F.col("id").desc())
)
