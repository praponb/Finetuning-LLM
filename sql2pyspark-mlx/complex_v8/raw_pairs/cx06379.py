from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_tariff = Window.partitionBy("site_tariff")
w_quality = Window.partitionBy("quality")
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_site_tariff_by_voltage = Window.partitionBy("site_tariff").orderBy(F.col("voltage").desc(), "id")
w_all_by_kwh_running = Window.orderBy(F.col("kwh").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_commissioned_on_year_by = Window.partitionBy(F.year("site_commissioned_on")).orderBy("kwh", "id").rowsBetween(-2, -1)
w_site_id_site_tariff_by_volta = Window.partitionBy("site_id", "site_tariff").orderBy(F.col("voltage_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_site_tariff_in_qual = Window.partitionBy("distinct_site_tariff_in_quality_site_id").orderBy(F.col("last_least_voltage_kwh").desc(), "id").rowsBetween(-2, -1)
w_distinct_site_tariff_in_qual_2 = Window.partitionBy("distinct_site_tariff_in_quality_site_id").orderBy(F.col("last_least_voltage_kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_voltage_ties = Window.orderBy(F.col("voltage").desc())
w_all_by_voltage_prior_6 = Window.orderBy(F.col("voltage").desc(), "id").rowsBetween(-6, -1)
w_distinct_site_id_in_site_tar = Window.partitionBy("distinct_site_id_in_site_tariff")

cleaned = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.least(F.col("voltage"), F.col("kwh")).alias("least_voltage_kwh"),
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
    )
)

joined = (
    cleaned.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.least_voltage_kwh",
        "s.voltage_filled",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        "si.site_name",
    )
)

windowed = (
    joined
    .select(
        "id",
        "kwh",
        "voltage",
        "quality",
        "least_voltage_kwh",
        "site_tariff",
        "site_commissioned_on",
        "site_name",
        F.size(F.collect_set("quality").over(w_site_tariff)).alias("distinct_quality_in_site_tariff"),
        F.count(F.lit(1)).over(w_site_tariff).alias("rows_in_site_tariff"),
        F.size(F.collect_set("site_tariff").over(w_quality)).alias("distinct_site_tariff_in_quality"),
        F.size(F.collect_set("site_id").over(w_site_tariff)).alias("distinct_site_id_in_site_tariff"),
        F.size(F.collect_set("site_tariff").over(w_quality_site_id)).alias("distinct_site_tariff_in_quality_site_id"),
        F.round(F.percent_rank().over(w_site_tariff_by_voltage), 3).alias("voltage_percent_rank"),
        F.min("kwh").over(w_all_by_kwh_running).alias("min_kwh_so_far"),
        F.max("kwh").over(w_site_commissioned_on_year_by).alias("kwh_max_2_prior"),
        F.row_number().over(w_site_tariff_by_voltage).alias("rn"),
        F.last("least_voltage_kwh").over(w_site_id_site_tariff_by_volta).alias("last_least_voltage_kwh"),
        F.count(F.lit(1)).over(w_quality_site_id).alias("rows_in_quality_site_id"),
    )
)

scored = windowed.select("*", F.length(F.trim(F.col("site_tariff"))).alias("site_tariff_len"))

windowed_2 = (
    scored
    .select(
        "id",
        "kwh",
        "quality",
        "least_voltage_kwh",
        "distinct_site_id_in_site_tariff",
        "distinct_site_tariff_in_quality_site_id",
        "voltage_percent_rank",
        "min_kwh_so_far",
        "kwh_max_2_prior",
        "last_least_voltage_kwh",
        F.max("least_voltage_kwh").over(w_distinct_site_tariff_in_qual).alias("least_voltage_kwh_max_2_prior"),
        F.sum("last_least_voltage_kwh").over(w_distinct_site_tariff_in_qual_2).alias("running_last_least_voltage_kwh"),
        F.round(F.cume_dist().over(w_all_by_voltage_ties), 3).alias("voltage_cume_dist"),
        F.max("kwh").over(w_all_by_voltage_prior_6).alias("kwh_max_6_prior"),
        F.last("voltage_percent_rank", ignorenulls=True).over(w_distinct_site_tariff_in_qual_2).alias("last_known_voltage_percent_rank"),
        F.round(F.col("min_kwh_so_far") - F.avg("min_kwh_so_far").over(w_distinct_site_id_in_site_tar), 2).alias("min_kwh_so_far_vs_distinct_site_id_in_site_tarif"),
    )
)

result = (
    windowed_2
    .select(
        "id",
        "kwh",
        "quality",
        "least_voltage_kwh",
        "distinct_site_id_in_site_tariff",
        "distinct_site_tariff_in_quality_site_id",
        "voltage_percent_rank",
        "last_least_voltage_kwh",
        "running_last_least_voltage_kwh",
        "voltage_cume_dist",
        "kwh_max_6_prior",
        "last_known_voltage_percent_rank",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.round(F.col("kwh_max_6_prior") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("kwh_max_6_prior_per_kwh"),
    )
)
