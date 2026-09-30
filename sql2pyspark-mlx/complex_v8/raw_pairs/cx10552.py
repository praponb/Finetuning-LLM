from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality_by_kwh = Window.partitionBy("site_id", "quality").orderBy("kwh", "id")
w_site_id_read_at_to_date_by_s = Window.partitionBy("site_id", F.to_date("read_at")).orderBy(F.col("site_installed_kw").desc_nulls_first(), "id")
w_read_at_to_date = Window.partitionBy(F.to_date("read_at"))
w_site_id = Window.partitionBy("site_id")
w_site_commissioned_on_year_si = Window.partitionBy(F.year("site_commissioned_on"), "site_id").orderBy(F.col("site_installed_kw").desc(), "id")

valid_readings = (
    df_meter_readings
    .filter((F.col("quality").isin("BAD", "OK")) & (F.hour(F.col("read_at")).between(10, 12)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
    )
)

enriched_site = (
    valid_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

ranked = (
    enriched_site
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        (F.col("site_installed_kw") - F.lag("site_installed_kw").over(w_site_id_quality_by_kwh)).alias("site_installed_kw_change"),
        F.lead("site_commissioned_on").over(w_site_id_read_at_to_date_by_s).alias("next_site_commissioned_on"),
        F.count(F.lit(1)).over(w_read_at_to_date).alias("rows_in_read_at_to_date"),
        F.round(F.avg("site_installed_kw").over(w_site_id), 2).alias("site_id_avg_site_installed_kw"),
        (F.col("kwh") - F.lag("kwh").over(w_site_commissioned_on_year_si)).alias("kwh_change"),
        F.min("site_installed_kw").over(w_read_at_to_date).alias("read_at_to_date_min_site_installed_kw"),
        F.lag("read_at").over(w_site_id_quality_by_kwh).alias("prev_read_at"),
        F.min("kwh").over(w_read_at_to_date).alias("read_at_to_date_min_kwh"),
    )
)

scored = ranked.withColumn("kwh_change_minus_kwh", F.col("kwh_change") - F.col("kwh"))

result = (
    scored
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "site_installed_kw_change",
        "next_site_commissioned_on",
        "rows_in_read_at_to_date",
        "site_id_avg_site_installed_kw",
        "kwh_change",
        "read_at_to_date_min_site_installed_kw",
        "read_at_to_date_min_kwh",
    )
    .orderBy(F.col("site_id").desc(), "id")
    .limit(5)
)
