from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id = Window.partitionBy("quality", "site_id")
w_all_by_reading_count_minus_m = Window.orderBy("reading_count_minus_max", "id")

base = (
    df_meter_readings
    .filter(F.col("voltage") <= 240.1)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "yyyy-MM-dd").alias("read_at_label"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.count(F.when(F.col("quality") == "BAD", 1)).alias("reading_quality_count"),
        F.count("*").alias("reading_count_2"),
    )
)

summarized_rows = (
    base.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
    )
)

quality_stats = (
    summarized_rows
    .groupBy("quality")
    .agg(F.max("reading_count").alias("max_reading_count"))
)

compared = (
    summarized_rows.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.reading_count") == F.col("g.max_reading_count"))
    .select(
        "s.*",
        "g.max_reading_count",
        F.round(F.col("s.reading_count") - F.col("g.max_reading_count"), 2).alias("reading_count_minus_max"),
    )
)

ranked = (
    compared
    .select(
        "id",
        "site_id",
        "voltage",
        "read_at_label",
        "reading_count",
        "reading_count_2",
        "reading_count_minus_max",
        F.round(F.col("reading_count_2") - F.avg("reading_count_2").over(w_quality_site_id), 2).alias("reading_count_2_vs_quality_site_id_avg"),
        F.ntile(10).over(w_all_by_reading_count_minus_m).alias("reading_count_minus_max_decile"),
    )
)

annotated = (
    ranked
    .select(
        "id",
        "site_id",
        "voltage",
        "read_at_label",
        "reading_count_2",
        "reading_count_minus_max",
        "reading_count_2_vs_quality_site_id_avg",
        F.coalesce(F.col("reading_count_2"), F.lit(0)).alias("reading_count_2_filled"),
    )
)

annotated_2 = (
    annotated
    .select(
        "id",
        "site_id",
        "voltage",
        "read_at_label",
        "reading_count_2_vs_quality_site_id_avg",
        "reading_count_2_filled",
        F.least(F.col("reading_count_2_filled"), F.col("voltage")).alias("least_reading_count_2_filled_voltage"),
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
        F.greatest(F.col("voltage"), F.col("reading_count_2_filled")).alias("greatest_voltage_reading_count_2_filled"),
    )
)

result = (
    annotated_2
    .select(
        "id",
        "site_id",
        "voltage",
        "voltage_filled",
        "greatest_voltage_reading_count_2_filled",
        F.lower(F.regexp_replace(F.trim(F.col("read_at_label")), " ", "_")).alias("read_at_label_slug"),
        F.lower(F.trim(F.col("read_at_label"))).alias("read_at_label_clean"),
    )
)
