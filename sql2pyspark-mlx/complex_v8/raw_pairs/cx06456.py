from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_first_reading_read_at_year_b = Window.partitionBy(F.year("first_reading_read_at")).orderBy(F.col("installed_kw").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_first_reading_read_at_year_b_2 = Window.partitionBy(F.year("first_reading_read_at")).orderBy(F.col("installed_kw").desc(), "id")
w_all_by_commissioned_on_runni = Window.orderBy("commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_sites = df_sites.filter(F.col("commissioned_on") >= F.lit("2024-06-27").cast("date"))

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.min("read_at").alias("first_reading_read_at"))
)

with_stats = (
    recent_sites.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select("s.*", "m.first_reading_read_at")
)

ranked = (
    with_stats
    .select(
        "*",
        F.first("commissioned_on").over(w_first_reading_read_at_year_b).alias("first_commissioned_on"),
        F.row_number().over(w_first_reading_read_at_year_b_2).alias("seq_no"),
        F.last("first_reading_read_at", ignorenulls=True).over(w_all_by_commissioned_on_runni).alias("last_known_first_reading_read_at"),
        F.round(F.percent_rank().over(w_first_reading_read_at_year_b_2), 3).alias("installed_kw_percent_rank"),
    )
)

tariff_stats = (
    ranked
    .groupBy("tariff")
    .agg(F.max("installed_kw").alias("max_installed_kw"))
)

outliers = (
    ranked.alias("s")
    .join(tariff_stats.alias("g"), F.col("g.tariff") == F.col("s.tariff"), "inner")
    .filter(F.col("s.installed_kw") == F.col("g.max_installed_kw"))
    .select(
        "s.*",
        "g.max_installed_kw",
        F.round(F.col("s.installed_kw") - F.col("g.max_installed_kw"), 2).alias("installed_kw_minus_max"),
    )
)

result = (
    outliers
    .filter(F.col("first_reading_read_at") < F.lit("2025-02-09 09:00:00").cast("timestamp"))
    .select(
        "id",
        "zone",
        "installed_kw",
        "first_reading_read_at",
        "first_commissioned_on",
        "seq_no",
        "last_known_first_reading_read_at",
        "installed_kw_percent_rank",
        "max_installed_kw",
        "installed_kw_minus_max",
        F.when(F.col("max_installed_kw") >= 425.8, "large").when(F.col("max_installed_kw") >= 128.8, "medium").otherwise("small").alias("max_installed_kw_tier"),
        F.round(F.col("seq_no") / F.nullif(F.col("installed_kw"), F.lit(0)), 3).alias("seq_no_per_installed_kw"),
    )
    .orderBy(F.col("id").desc_nulls_first())
    .limit(5)
)
