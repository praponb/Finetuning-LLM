from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reading_id = Window.partitionBy("reading_id")
w_reading_id_tariff_by_reading = Window.partitionBy("reading_id", "tariff").orderBy("reading_read_at", "id", "reading_id").rowsBetween(-3, Window.currentRow)
w_all_by_reading_read_at_year = Window.orderBy("reading_read_at_year", "id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_reading_read_at_year_2 = Window.orderBy(F.col("reading_read_at_year").desc_nulls_first(), "id", "reading_id")
w_reading_quality_reading_read = Window.partitionBy("reading_quality", "reading_read_at_year")
w_all_by_reading_read_at_year_3 = Window.orderBy("reading_read_at_year", "id", "reading_id").rowsBetween(-6, Window.currentRow)
w_all_by_reading_read_at_year_4 = Window.orderBy("reading_read_at_year", "id", "reading_id").rowsBetween(-2, -1)
w_all_by_reading_read_at_year_5 = Window.orderBy("reading_read_at_year")

recent = (
    df_sites
    .filter((F.col("tariff").isin("TOU", "EV")) | (F.year(F.col("commissioned_on")) == 2024))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "commissioned_on",
    )
)

with_children = (
    recent.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.site_name",
        "s.tariff",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
    )
)

qualified = (
    with_children.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.id"), "left_semi")
    .filter(F.col("reading_id").isNotNull())
)

scored = (
    qualified
    .select(
        "*",
        F.year(F.col("reading_read_at")).alias("reading_read_at_year"),
        F.lower(F.regexp_replace(F.trim(F.col("site_name")), " ", "_")).alias("site_name_slug"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.count(F.lit(1)).over(w_reading_id).alias("rows_in_reading_id"),
        F.round(F.avg("reading_read_at_year").over(w_reading_id_tariff_by_reading), 2).alias("reading_read_at_year_avg_4"),
        F.sum("reading_read_at_year").over(w_all_by_reading_read_at_year).alias("running_reading_read_at_year"),
        F.round(F.percent_rank().over(w_all_by_reading_read_at_year_2), 3).alias("reading_read_at_year_percent_rank"),
    )
)

ranked = (
    with_windows
    .select(
        "*",
        F.max("reading_read_at_year").over(w_reading_quality_reading_read).alias("reading_quality_reading_read_at_year_max_reading"),
        F.round(F.col("reading_read_at_year") - F.avg("reading_read_at_year").over(w_reading_quality_reading_read), 2).alias("reading_read_at_year_vs_reading_quality_reading_"),
        F.round(F.avg("reading_read_at_year_avg_4").over(w_all_by_reading_read_at_year_3), 2).alias("reading_read_at_year_avg_4_avg_7"),
        F.max("rows_in_reading_id").over(w_all_by_reading_read_at_year_4).alias("rows_in_reading_id_max_2_prior"),
        F.rank().over(w_all_by_reading_read_at_year_5).alias("reading_read_at_year_rank"),
    )
)

result = (
    ranked
    .groupBy("reading_id", "tariff")
    .pivot("reading_quality", ["BAD", "OK", "EST"])
    .agg(F.min("reading_read_at_year"))
    .orderBy("reading_id", "tariff")
)
