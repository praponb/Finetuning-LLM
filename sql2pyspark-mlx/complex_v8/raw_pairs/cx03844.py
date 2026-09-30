from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id_by_voltage = Window.partitionBy("quality", "site_id").orderBy("voltage", "id")
w_read_at_year = Window.partitionBy("read_at_year")
w_quality_site_id_by_voltage_t = Window.partitionBy("quality", "site_id").orderBy("voltage")
w_quality_site_id_by_voltage_f = Window.partitionBy("quality", "site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_readings = (
    df_meter_readings
    .filter((F.col("site_id") <= 3) & (F.col("quality").isin("OK", "EST")))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.year(F.col("read_at")).alias("read_at_year"),
        F.quarter(F.col("read_at")).alias("read_at_quarter"),
    )
)

sequenced = (
    prepared_readings
    .select(
        "*",
        F.row_number().over(w_quality_site_id_by_voltage).alias("rn"),
        F.round(F.col("read_at_quarter") / F.nullif(F.sum("read_at_quarter").over(w_read_at_year), F.lit(0)), 4).alias("read_at_quarter_share_of_read_at_year"),
        F.size(F.collect_set("site_id").over(w_read_at_year)).alias("distinct_site_id_in_read_at_year"),
        F.rank().over(w_quality_site_id_by_voltage_t).alias("voltage_rank"),
        F.first("read_at_quarter").over(w_quality_site_id_by_voltage_f).alias("first_read_at_quarter"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("read_at").alias("last_reading_read_at"),
        F.count("*").alias("reading_count"),
        F.max("voltage").alias("max_reading_voltage"),
    )
)

profiled = (
    sequenced.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.last_reading_read_at",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.max_reading_voltage",
    )
)

result = (
    profiled
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        "quality_slug",
        "read_at_year",
        "read_at_quarter",
        "rn",
        "distinct_site_id_in_read_at_year",
        "last_reading_read_at",
        "max_reading_voltage",
        F.least(F.col("first_read_at_quarter"), F.col("kwh")).alias("least_first_read_at_quarter_kwh"),
        F.least(F.col("read_at_year"), F.col("first_read_at_quarter")).alias("least_read_at_year_first_read_at_quarter"),
    )
    .orderBy(F.col("least_read_at_year_first_read_at_quarter").desc())
)
