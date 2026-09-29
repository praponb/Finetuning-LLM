from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_year_site_id = Window.partitionBy(F.year("read_at"), "site_id")
w_read_at_month_quality_by_day = Window.partitionBy(F.date_trunc("month", "read_at"), "quality").orderBy(F.col("days_since_read_at").desc_nulls_first())
w_read_at_month_quality_by_day_2 = Window.partitionBy(F.date_trunc("month", "read_at"), "quality").orderBy(F.col("days_since_read_at").desc_nulls_first(), "id")

valid_readings = (
    df_meter_readings
    .filter(F.col("quality") == "BAD")
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

metrics = (
    valid_readings
    .select(
        "*",
        F.sum("days_since_read_at").over(w_read_at_year_site_id).alias("read_at_year_site_id_sum_days_since_read_at"),
        F.round(F.percent_rank().over(w_read_at_month_quality_by_day), 3).alias("days_since_read_at_percent_rank"),
        F.count(F.lit(1)).over(w_read_at_year_site_id).alias("rows_in_read_at_year_site_id"),
        F.ntile(10).over(w_read_at_month_quality_by_day_2).alias("days_since_read_at_decile"),
    )
)

with_details_site = (
    metrics.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") <= 386.6), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        "s.days_since_read_at",
        "s.days_since_read_at_percent_rank",
        "s.rows_in_read_at_year_site_id",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

result = (
    with_details_site
    .groupBy("site_commissioned_on")
    .pivot("quality", ["EST", "BAD", "OK"])
    .agg(F.max("days_since_read_at").alias("max_v"), F.sum("days_since_read_at").alias("sum_v"))
)
