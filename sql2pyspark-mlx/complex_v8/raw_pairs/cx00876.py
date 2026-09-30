from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("zone").orderBy(F.col("installed_kw").desc(), F.col("id").desc())
w_zone = Window.partitionBy("zone")
w_commissioned_on_month_end_mo = Window.partitionBy(F.date_trunc("month", "commissioned_on_month_end"), "zone").orderBy("installed_kw_minus_avg")
w_commissioned_on_month_end_mo_2 = Window.partitionBy(F.date_trunc("month", "commissioned_on_month_end"), "zone").orderBy("installed_kw_minus_avg", "id")

valid = (
    df_sites
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        "commissioned_on",
    )
)

zone_stats = (
    valid
    .groupBy("zone")
    .agg(F.avg("installed_kw").alias("avg_installed_kw"))
)

compared = (
    valid.alias("s")
    .join(zone_stats.alias("g"), F.col("g.zone") == F.col("s.zone"), "inner")
    .filter(F.col("s.installed_kw") > F.col("g.avg_installed_kw"))
    .select(
        "s.*",
        "g.avg_installed_kw",
        F.round(F.col("s.installed_kw") - F.col("g.avg_installed_kw"), 2).alias("installed_kw_minus_avg"),
    )
)

without_matches = (
    compared.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.col("m.quality").isin("EST", "OK")) & (F.col("m.voltage").between(216.5, 233.1)), "left_semi")
)

unique_rows = (
    without_matches
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

scored = (
    unique_rows
    .select(
        "*",
        F.last_day(F.col("commissioned_on")).alias("commissioned_on_month_end"),
        F.concat(F.col("site_name"), F.lit("#"), F.col("id").cast("string")).alias("site_name_ref"),
    )
)

with_trends = (
    scored
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        "commissioned_on",
        "avg_installed_kw",
        "installed_kw_minus_avg",
        "commissioned_on_month_end",
        "site_name_ref",
        F.count(F.lit(1)).over(w_zone).alias("rows_in_zone"),
        F.round(F.percent_rank().over(w_commissioned_on_month_end_mo), 3).alias("installed_kw_minus_avg_percent_rank"),
        F.ntile(10).over(w_commissioned_on_month_end_mo_2).alias("installed_kw_minus_avg_decile"),
    )
)

annotated = (
    with_trends
    .select(
        "*",
        F.date_format(F.col("commissioned_on"), "yyyy-MM-dd").alias("commissioned_on_label"),
        F.lower(F.regexp_replace(F.trim(F.col("site_name")), " ", "_")).alias("site_name_slug"),
        F.when(F.col("avg_installed_kw") >= 376.2, "large").when(F.col("avg_installed_kw") >= 178.2, "medium").otherwise("small").alias("avg_installed_kw_tier"),
    )
)

result = (
    annotated
    .filter(F.col("avg_installed_kw_tier") == "medium")
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        "commissioned_on",
        "avg_installed_kw",
        "installed_kw_minus_avg",
        "site_name_ref",
        "rows_in_zone",
        "commissioned_on_label",
        "site_name_slug",
        "avg_installed_kw_tier",
        F.concat_ws("|", F.col("commissioned_on_label"), F.col("site_name_slug")).alias("commissioned_on_label_site_name_slug_label"),
        F.greatest(F.col("rows_in_zone"), F.col("installed_kw_minus_avg")).alias("greatest_rows_in_zone_installed_kw_minus_avg"),
    )
    .orderBy(
        F.col("commissioned_on_label_site_name_slug_label").desc_nulls_first(),
        "avg_installed_kw_tier",
    )
)
