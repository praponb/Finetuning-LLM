from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality_len = Window.partitionBy("site_id", "quality_len")
w_quality_len_site_id_by_read = Window.partitionBy("quality_len", "site_id").orderBy("read_at_year")
w_quality_len_site_id_by_read_2 = Window.partitionBy("quality_len", "site_id").orderBy("read_at_year", "id")

recent_readings = (
    df_meter_readings
    .filter((F.col("kwh") > 33.9) & (F.col("read_at") < F.lit("2025-02-24 14:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.year(F.col("read_at")).alias("read_at_year"),
    )
)

with_trends = (
    recent_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "days_since_read_at",
        "quality_len",
        F.max("days_since_read_at").over(w_site_id_quality_len).alias("site_id_quality_len_max_days_since_read_at"),
        F.count(F.lit(1)).over(w_site_id_quality_len).alias("rows_in_site_id_quality_len"),
        F.max("voltage").over(w_site_id_quality_len).alias("site_id_quality_len_max_voltage"),
        F.dense_rank().over(w_quality_len_site_id_by_read).alias("read_at_year_dense_rank"),
        F.size(F.collect_set("quality").over(w_site_id_quality_len)).alias("distinct_quality_in_site_id_quality_len"),
        F.lag("voltage", 2).over(w_quality_len_site_id_by_read_2).alias("prev2_voltage"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality") == "EST", 1)).alias("reading_quality_count"),
    )
)

summarized_rows = (
    with_trends.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
    )
)

with_parent = (
    summarized_rows.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.days_since_read_at",
        "s.quality_len",
        "s.site_id_quality_len_max_days_since_read_at",
        "s.site_id_quality_len_max_voltage",
        "s.distinct_quality_in_site_id_quality_len",
        "s.prev2_voltage",
        "s.reading_quality_count",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone"),
        "si.site_name",
    )
)

result = (
    with_parent
    .cube("distinct_quality_in_site_id_quality_len", "quality_len")
    .agg(
        F.grouping_id().alias("gid"),
        F.sum(F.when(F.col("site_zone") != "Z3", F.col("reading_quality_count"))).alias("reading_quality_count_site_zone_z3"),
        F.array_sort(F.collect_set("site_name")).alias("site_name_list"),
        F.count("*").alias("cnt"),
    )
)
