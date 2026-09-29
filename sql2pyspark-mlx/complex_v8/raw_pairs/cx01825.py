from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_read_at = Window.partitionBy("site_id").orderBy("read_at", "id")
w_all_by_voltage_running = Window.orderBy(F.col("voltage").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_len_by_read_at_movin = Window.partitionBy("quality_len").orderBy("read_at", "id").rowsBetween(-6, Window.currentRow)
w_site_id_site_zone = Window.partitionBy("site_id", "site_zone")

recent = (
    df_meter_readings
    .filter((F.col("kwh") <= 110.5) | (F.month(F.col("read_at")).isin(2, 3)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

ranked = (
    recent
    .select(
        "*",
        F.round(F.cume_dist().over(w_site_id_by_read_at), 3).alias("read_at_cume_dist"),
        F.first("read_at", ignorenulls=True).over(w_all_by_voltage_running).alias("first_known_read_at"),
        F.rank().over(w_site_id_by_read_at).alias("read_at_rank"),
    )
)

labeled = (
    ranked
    .filter(F.col("read_at_cume_dist") >= 0.1)
    .select(
        "*",
        F.month(F.col("first_known_read_at")).alias("first_known_read_at_month"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.length(F.trim(F.col("quality"))).alias("quality_len_2"),
    )
)

with_details = (
    labeled.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.read_at_cume_dist",
        "s.first_known_read_at",
        "s.read_at_rank",
        "s.quality_len",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone"),
    )
)

ranked_2 = (
    with_details
    .select(
        "id",
        "read_at",
        "voltage",
        "read_at_cume_dist",
        "first_known_read_at",
        "quality_len",
        "site_installed_kw",
        "site_commissioned_on",
        F.sum("read_at_rank").over(w_quality_len_by_read_at_movin).alias("read_at_rank_sum_7"),
        F.max("site_installed_kw").over(w_site_id_site_zone).alias("site_id_site_zone_max_site_installed_kw"),
    )
)

summary = (
    ranked_2
    .groupBy(
        F.date_trunc("month", "first_known_read_at").alias("first_known_read_at_month"),
        "quality_len",
        F.date_format("site_commissioned_on", "yyyy-MM").alias("site_commissioned_on_ym"),
    )
    .agg(
        F.max("read_at_cume_dist").alias("max_read_at_cume_dist"),
        F.round(F.sum("site_id_site_zone_max_site_installed_kw") / F.nullif(F.sum("voltage"), F.lit(0)), 3).alias("site_id_site_zone_max_site_installed_kw_to_volta"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("site_installed_kw", 0.5).alias("median_site_installed_kw"),
        F.max("read_at_rank_sum_7").alias("max_read_at_rank_sum_7"),
        F.max("site_installed_kw").alias("max_site_installed_kw_check"),
    )
    .filter(F.col("max_site_installed_kw_check") > 141.6)
    .drop("max_site_installed_kw_check")
)

result = (
    summary
    .select(
        "first_known_read_at_month",
        "quality_len",
        "site_commissioned_on_ym",
        "max_read_at_cume_dist",
        "site_id_site_zone_max_site_installed_kw_to_volta",
        "max_read_at_rank_sum_7",
    )
    .orderBy(
        F.col("max_read_at_cume_dist").desc(),
        "site_commissioned_on_ym",
        "first_known_read_at_month",
        "quality_len",
    )
    .limit(10)
)
