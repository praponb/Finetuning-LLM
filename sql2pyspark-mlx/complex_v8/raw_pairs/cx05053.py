from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_site_commissioned_on = Window.partitionBy("site_id", F.month("site_commissioned_on"))
w_all_by_site_installed_kw = Window.orderBy("site_installed_kw", "id")
w_all_by_site_installed_kw_run = Window.orderBy(F.col("site_installed_kw").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_installed_kw_rank = Window.partitionBy("site_installed_kw_rank")

valid = (
    df_meter_readings
    .filter(F.col("kwh") >= 35.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.when(F.col("quality").isin("OK", "EST"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
    )
)

with_details = (
    valid.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.commissioned_on") < F.lit("2024-11-21").cast("date"))
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "si.site_name",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

with_windows = (
    with_details
    .select(
        "*",
        F.min("voltage").over(w_site_id_site_commissioned_on).alias("site_id_site_commissioned_on_month_min_voltage"),
        F.rank().over(w_all_by_site_installed_kw).alias("site_installed_kw_rank"),
        F.ntile(4).over(w_all_by_site_installed_kw).alias("site_installed_kw_quartile"),
        F.ntile(3).over(w_all_by_site_installed_kw).alias("site_installed_kw_tercile"),
        F.count(F.lit(1)).over(w_site_id_site_commissioned_on).alias("rows_in_site_id_site_commissioned_on_month"),
        F.first("voltage", ignorenulls=True).over(w_all_by_site_installed_kw_run).alias("first_known_voltage"),
        F.row_number().over(w_all_by_site_installed_kw).alias("seq_no"),
        F.max("voltage").over(w_site_id_site_commissioned_on).alias("site_id_site_commissioned_on_month_max_voltage"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id_site_commissioned_on), F.lit(0)), 4).alias("voltage_share_of_site_id_site_commissioned_on_mo"),
    )
)

first_rows = (
    with_windows
    .filter(F.col("seq_no") <= 5)
    .drop("seq_no")
)

grouped = (
    first_rows
    .groupBy(
        "site_installed_kw_quartile",
        F.when(F.col("kwh") >= 109.0, "HIGH").when(F.col("kwh") >= 29.2, "MEDIUM").otherwise("LOW").alias("kwh_tier"),
        "site_installed_kw_rank",
    )
    .agg(
        F.round(F.sum("kwh"), 2).alias("total_kwh"),
        F.sum(F.when(F.upper(F.trim(F.col("site_name"))).like("H%"), F.col("site_id_site_commissioned_on_month_max_voltage")).otherwise(0)).alias("site_id_site_commissioned_on_month_max_voltage_u"),
        F.percentile_approx("site_installed_kw_rank", 0.5).alias("median_site_installed_kw_rank"),
        F.sum(F.when(F.lower(F.col("site_name")).like("%ot%"), F.col("site_id_site_commissioned_on_month_min_voltage")).otherwise(0)).alias("site_id_site_commissioned_on_month_min_voltage_l"),
        F.sum("voltage_share_of_site_id_site_commissioned_on_mo").alias("sum_voltage_share_of_site_id_site_commissioned_o"),
    )
    .filter(F.col("sum_voltage_share_of_site_id_site_commissioned_o") > 0.2)
    .drop("sum_voltage_share_of_site_id_site_commissioned_o")
)

ranked = (
    grouped
    .select(
        "site_installed_kw_quartile",
        "kwh_tier",
        "site_installed_kw_rank",
        "total_kwh",
        "site_id_site_commissioned_on_month_max_voltage_u",
        "median_site_installed_kw_rank",
        "site_id_site_commissioned_on_month_min_voltage_l",
        F.round(F.col("total_kwh") / F.nullif(F.sum("total_kwh").over(w_site_installed_kw_rank), F.lit(0)), 4).alias("total_kwh_share_of_site_installed_kw_rank"),
        F.round(F.col("site_installed_kw_rank") / F.nullif(F.sum("site_installed_kw_rank").over(w_site_installed_kw_rank), F.lit(0)), 4).alias("site_installed_kw_rank_share_of_site_installed_k"),
    )
)

result = (
    ranked
    .select(
        "site_installed_kw_quartile",
        "kwh_tier",
        "site_installed_kw_rank",
        "total_kwh",
        "site_id_site_commissioned_on_month_max_voltage_u",
        "median_site_installed_kw_rank",
        "site_id_site_commissioned_on_month_min_voltage_l",
        "total_kwh_share_of_site_installed_kw_rank",
        "site_installed_kw_rank_share_of_site_installed_k",
        F.length(F.trim(F.col("kwh_tier"))).alias("kwh_tier_len"),
    )
    .orderBy("total_kwh_share_of_site_installed_kw_rank")
)
