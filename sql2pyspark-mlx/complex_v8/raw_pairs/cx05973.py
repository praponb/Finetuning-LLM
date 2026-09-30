from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id_by_site_comm = Window.partitionBy("quality", "site_id").orderBy("site_commissioned_on", "id")
w_site_commissioned_on_month_b = Window.partitionBy(F.month("site_commissioned_on")).orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id = Window.partitionBy("site_id")
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_site_id_quality_by_site_comm = Window.partitionBy("site_id", "quality").orderBy("site_commissioned_on", "id")
w_site_commissioned_on_month_b_2 = Window.partitionBy(F.month("site_commissioned_on")).orderBy(F.col("voltage").desc(), "id")
w_quality_site_id_by_site_comm_2 = Window.partitionBy("quality", "site_id").orderBy("site_commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
    )
)

with_details_site = (
    filtered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        "s.quality_ref",
        "s.quality_prefix",
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

windowed = (
    with_details_site
    .select(
        "id",
        "voltage",
        "quality",
        "quality_prefix",
        "site_name",
        "site_installed_kw",
        F.row_number().over(w_quality_site_id_by_site_comm).alias("seq_no"),
        F.first("site_commissioned_on", ignorenulls=True).over(w_site_commissioned_on_month_b).alias("first_known_site_commissioned_on"),
        F.round(F.col("site_installed_kw") / F.nullif(F.sum("site_installed_kw").over(w_site_id), F.lit(0)), 4).alias("site_installed_kw_share_of_site_id"),
        F.min("site_installed_kw").over(w_quality_site_id).alias("quality_site_id_min_site_installed_kw"),
        F.round(F.cume_dist().over(w_site_id_quality_by_site_comm), 3).alias("site_commissioned_on_cume_dist"),
        F.ntile(3).over(w_site_commissioned_on_month_b_2).alias("voltage_tercile"),
        F.rank().over(w_quality_site_id_by_site_comm).alias("site_commissioned_on_rank"),
        F.max("voltage").over(w_quality_site_id_by_site_comm_2).alias("max_voltage_so_far"),
        F.ntile(5).over(w_quality_site_id_by_site_comm).alias("site_commissioned_on_quintile"),
        F.datediff(F.col("site_commissioned_on"), F.lag("site_commissioned_on").over(w_quality_site_id_by_site_comm)).alias("days_since_prev_site_commissioned_on"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_quality_site_id), F.lit(0)), 4).alias("voltage_share_of_quality_site_id"),
        F.sum("site_installed_kw").over(w_quality_site_id).alias("quality_site_id_sum_site_installed_kw"),
    )
)

leaders = (
    windowed
    .filter(F.col("site_commissioned_on_rank") == 1)
    .drop("site_commissioned_on_rank")
)

result = (
    leaders
    .filter(F.col("site_installed_kw").between(66.5, 373.3))
    .select(
        "id",
        "voltage",
        "quality",
        "quality_prefix",
        "site_installed_kw",
        "seq_no",
        "first_known_site_commissioned_on",
        "site_installed_kw_share_of_site_id",
        "quality_site_id_min_site_installed_kw",
        "site_commissioned_on_cume_dist",
        "voltage_tercile",
        "max_voltage_so_far",
        "days_since_prev_site_commissioned_on",
        "voltage_share_of_quality_site_id",
        "quality_site_id_sum_site_installed_kw",
        F.coalesce(F.col("seq_no"), F.lit(0)).alias("seq_no_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("site_name")), " ", "_")).alias("site_name_slug"),
    )
    .orderBy(F.col("quality").desc())
)
