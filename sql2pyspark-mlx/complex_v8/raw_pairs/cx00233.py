from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_distinct_site_ids_by_voltage = Window.partitionBy("distinct_site_ids").orderBy("voltage_quality_est", "read_at_year", "quality", "site_id").rowsBetween(-1, -1)
w_distinct_site_ids_by_voltage_2 = Window.partitionBy("distinct_site_ids").orderBy("voltage_quality_est", "read_at_year", "quality", "site_id")
w_all_by_distinct_site_ids = Window.orderBy("distinct_site_ids", "read_at_year", "quality", "site_id")

filtered = (
    df_meter_readings
    .filter((F.col("kwh") < 89.8) & (F.col("voltage") >= 213.7))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

per_group = (
    filtered
    .groupBy(F.year("read_at").alias("read_at_year"), "quality", "site_id")
    .agg(
        F.countDistinct("site_id").alias("distinct_site_ids"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("voltage").alias("min_voltage"),
        F.sum(F.when(F.col("quality") == "EST", F.col("voltage")).otherwise(0)).alias("voltage_quality_est"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.min("read_at").alias("first_read_at"),
    )
)

windowed = (
    per_group
    .select(
        "read_at_year",
        "quality",
        "site_id",
        "distinct_site_ids",
        "min_voltage",
        "voltage_quality_est",
        "first_read_at",
        F.max("distinct_ids").over(w_site_id).alias("site_id_max_distinct_ids"),
        F.max("distinct_ids").over(w_distinct_site_ids_by_voltage).alias("distinct_ids_max_1_prior"),
        F.ntile(5).over(w_distinct_site_ids_by_voltage_2).alias("voltage_quality_est_quintile"),
        F.ntile(3).over(w_all_by_distinct_site_ids).alias("distinct_site_ids_tercile"),
        F.sum("read_at_year").over(w_site_id).alias("site_id_sum_read_at_year"),
        F.round(F.percent_rank().over(w_all_by_distinct_site_ids), 3).alias("distinct_site_ids_percent_rank"),
    )
)

filtered_2 = (
    windowed.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.tariff") == "FLAT") & (F.col("si.commissioned_on") < F.lit("2024-11-21").cast("date")), "left_anti")
)

derived = filtered_2.select("*", F.length(F.trim(F.col("quality"))).alias("quality_len"))

filtered_3 = (
    derived.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality") == "OK") & (F.col("m.read_at") >= F.lit("2025-02-06 09:00:00").cast("timestamp")), "left_anti")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.min("kwh").alias("min_reading_kwh"))
)

profiled = (
    filtered_3.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.min_reading_kwh")
)

result = (
    profiled
    .select(
        "read_at_year",
        "quality",
        F.col("min_voltage").alias("m_min_voltage"),
        F.col("distinct_site_ids_percent_rank").alias("m_distinct_site_ids_percent_rank"),
    )
    .unpivot(
        ids=["read_at_year", "quality"],
        values=["m_min_voltage", "m_distinct_site_ids_percent_rank"],
        variableColumnName="metric",
        valueColumnName="value",
    )
)
