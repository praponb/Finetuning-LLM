from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_zone_site_tariff = Window.partitionBy("site_zone", "site_tariff")
w_site_tariff_site_zone = Window.partitionBy("site_tariff", "site_zone")
w_site_tariff_by_voltage = Window.partitionBy("site_tariff").orderBy(F.col("voltage").desc(), "id")
w_all_by_voltage = Window.orderBy(F.col("voltage").desc_nulls_first(), "id")
w_all_by_kwh_running = Window.orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_site_zone_by_voltage = Window.partitionBy("site_id", "site_zone").orderBy("voltage", "id")
w_site_id = Window.partitionBy("site_id")
w_site_id_site_zone_by_voltage_2 = Window.partitionBy("site_id", "site_zone").orderBy("voltage", "id").rowsBetween(-1, Window.currentRow)
w_site_zone_site_id_by_voltage = Window.partitionBy("site_zone", "site_id").orderBy("voltage", "id")
w_site_zone_site_id_by_voltage_2 = Window.partitionBy("site_zone", "site_id").orderBy("voltage", "id").rowsBetween(-6, -1)
w_distinct_site_id_in_site_tar = Window.partitionBy("distinct_site_id_in_site_tariff_site_zone")
w_all_by_kwh_avg_2_ties = Window.orderBy("kwh_avg_2")
w_all_by_kwh_avg_2_moving_3 = Window.orderBy("kwh_avg_2", "id").rowsBetween(-3, Window.currentRow)
w_all_by_kwh_avg_2_moving_1 = Window.orderBy("kwh_avg_2", "id").rowsBetween(-1, Window.currentRow)

cleaned_readings = (
    df_meter_readings
    .filter(F.col("site_id") <= 5)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

enriched = (
    cleaned_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") > 96.9), "inner")
    .filter(F.col("si.zone") == "Z4")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "si.site_name",
        F.col("si.zone").alias("site_zone"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

ranked = (
    enriched
    .select(
        "*",
        F.size(F.collect_set("site_id").over(w_site_zone_site_tariff)).alias("distinct_site_id_in_site_zone_site_tariff"),
        F.max("kwh").over(w_site_tariff_site_zone).alias("site_tariff_site_zone_max_kwh"),
        F.dense_rank().over(w_site_tariff_by_voltage).alias("voltage_dense_rank"),
        F.dense_rank().over(w_all_by_voltage).alias("voltage_dense_rank_2"),
        F.count(F.lit(1)).over(w_site_zone_site_tariff).alias("rows_in_site_zone_site_tariff"),
        F.size(F.collect_set("site_id").over(w_site_tariff_site_zone)).alias("distinct_site_id_in_site_tariff_site_zone"),
        F.max("voltage").over(w_all_by_kwh_running).alias("max_voltage_so_far"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_zone_site_tariff), F.lit(0)), 4).alias("voltage_share_of_site_zone_site_tariff"),
        (F.col("voltage") - F.lag("voltage").over(w_site_id_site_zone_by_voltage)).alias("voltage_change"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.round(F.avg("kwh").over(w_site_id_site_zone_by_voltage_2), 2).alias("kwh_avg_2"),
        F.ntile(5).over(w_site_zone_site_id_by_voltage).alias("voltage_quintile"),
        F.sum("kwh").over(w_site_zone_site_id_by_voltage_2).alias("kwh_sum_6_prior"),
    )
)

derived = (
    ranked
    .filter(F.col("voltage_quintile") >= 3)
    .select(
        "id",
        "site_id",
        "voltage",
        "site_zone",
        "distinct_site_id_in_site_zone_site_tariff",
        "voltage_dense_rank",
        "rows_in_site_zone_site_tariff",
        "distinct_site_id_in_site_tariff_site_zone",
        "voltage_share_of_site_zone_site_tariff",
        "voltage_change",
        "rows_in_site_id",
        "kwh_avg_2",
        "voltage_quintile",
        F.concat_ws(" / ", F.col("site_zone"), F.col("site_name")).alias("site_zone_site_name_label"),
        F.lower(F.trim(F.col("site_name"))).alias("site_name_clean"),
        (F.floor(F.col("max_voltage_so_far") / 500) * 500).alias("max_voltage_so_far_bucket"),
    )
)

windowed = (
    derived
    .select(
        "*",
        F.round(F.avg("max_voltage_so_far_bucket").over(w_distinct_site_id_in_site_tar), 2).alias("distinct_site_id_in_site_tariff_site_zone_avg_ma"),
        F.rank().over(w_all_by_kwh_avg_2_ties).alias("kwh_avg_2_rank"),
        F.size(F.collect_set("site_zone").over(w_distinct_site_id_in_site_tar)).alias("distinct_site_zone_in_distinct_site_id_in_site_t"),
        F.round(F.avg("rows_in_site_id").over(w_all_by_kwh_avg_2_moving_3), 2).alias("rows_in_site_id_avg_4"),
        F.max("max_voltage_so_far_bucket").over(w_all_by_kwh_avg_2_moving_1).alias("max_voltage_so_far_bucket_max_2"),
        F.dense_rank().over(w_all_by_kwh_avg_2_ties).alias("kwh_avg_2_dense_rank"),
        F.count(F.lit(1)).over(w_distinct_site_id_in_site_tar).alias("rows_in_distinct_site_id_in_site_tariff_site_zon"),
    )
)

result = (
    windowed
    .filter(F.col("distinct_site_id_in_site_tariff_site_zone") >= 3)
    .select(
        "id",
        "site_id",
        "voltage",
        "site_zone",
        "voltage_dense_rank",
        "rows_in_site_zone_site_tariff",
        "distinct_site_id_in_site_tariff_site_zone",
        "voltage_share_of_site_zone_site_tariff",
        "voltage_change",
        "rows_in_site_id",
        "kwh_avg_2",
        "site_zone_site_name_label",
        "site_name_clean",
        "max_voltage_so_far_bucket",
        "distinct_site_id_in_site_tariff_site_zone_avg_ma",
        "kwh_avg_2_rank",
        "max_voltage_so_far_bucket_max_2",
        "kwh_avg_2_dense_rank",
        "rows_in_distinct_site_id_in_site_tariff_site_zon",
        F.substring(F.trim(F.col("site_zone")), 1, 2).alias("site_zone_prefix"),
        (F.col("voltage_dense_rank") + F.col("kwh_avg_2_rank")).alias("voltage_dense_rank_plus_kwh_avg_2_rank"),
    )
    .orderBy("kwh_avg_2", F.col("site_zone_site_name_label").desc(), "id")
    .limit(5)
)
