from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_year_month_by_read_a = Window.partitionBy(F.month("read_at_year")).orderBy("read_at", "id")
w_site_id_quality_by_voltage = Window.partitionBy("site_id", "quality").orderBy("voltage", "id")

base_readings = (
    df_meter_readings
    .filter((F.col("read_at") >= F.lit("2025-01-19 09:00:00").cast("timestamp")) & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("year", F.col("read_at")).alias("read_at_year"),
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
        F.abs(F.col("voltage") - 222.3).alias("voltage_distance"),
    )
)

with_trends = (
    base_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "read_at_year",
        "voltage_distance",
        F.ntile(4).over(w_read_at_year_month_by_read_a).alias("read_at_quartile"),
        F.lead("voltage_distance").over(w_site_id_quality_by_voltage).alias("next_voltage_distance"),
    )
)

per_group = (
    with_trends
    .groupBy("site_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
        F.count(F.when(F.col("quality").isNotNull(), 1)).alias("n_quality_is_not_null"),
        F.sum(F.when(F.col("quality").isNotNull(), F.col("read_at_quartile"))).alias("read_at_quartile_quality_is_not_null"),
    )
)

screened = (
    per_group.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
    .filter(F.col("read_at_quartile_quality_is_not_null") >= 5)
)

joined = (
    screened.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.zone") == "Z2"), "inner")
    .filter(F.col("si.zone") == "Z3")
    .select(
        "s.site_id",
        "s.n_rows",
        "s.distinct_site_ids",
        "s.n_quality_is_not_null",
        "s.read_at_quartile_quality_is_not_null",
        "si.site_name",
        F.col("si.zone").alias("site_zone"),
    )
)

result = (
    joined
    .select(
        "site_id",
        "n_rows",
        "n_quality_is_not_null",
        "read_at_quartile_quality_is_not_null",
        "site_name",
    )
    .orderBy(F.col("site_name").desc(), "site_id")
    .limit(5)
)
