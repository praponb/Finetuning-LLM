from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id_by_voltage = Window.partitionBy("quality", "site_id").orderBy("voltage", "id")
w_quality_by_site_installed_kw = Window.partitionBy("quality").orderBy(F.col("site_installed_kw").desc(), "id")
w_all_by_voltage = Window.orderBy("voltage", "id")
w_all_by_voltage_moving_2 = Window.orderBy("voltage", "id").rowsBetween(-2, Window.currentRow)
w_site_id = Window.partitionBy("site_id")
w_voltage_tercile_distinct_qua = Window.partitionBy("voltage_tercile", "distinct_quality_in_site_id").orderBy(F.col("distinct_quality_in_site_id").desc(), "id").rowsBetween(-1, -1)
w_voltage_tercile_by_next_read = Window.partitionBy("voltage_tercile").orderBy(F.col("next_read_at").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_all_by_site_installed_kw_qua = Window.orderBy("site_installed_kw_quartile", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "MMM yyyy").alias("read_at_label"),
        F.round(F.col("kwh") * 0.25, 2).alias("kwh_adj"),
    )
)

joined = (
    prepared.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.read_at_label",
        "s.kwh_adj",
        F.col("si.installed_kw").alias("site_installed_kw"),
        "si.site_name",
    )
)

with_trends = (
    joined
    .select(
        "id",
        "voltage",
        "quality",
        "read_at_label",
        "kwh_adj",
        "site_installed_kw",
        F.lead("read_at", 3).over(w_quality_site_id_by_voltage).alias("next3_read_at"),
        F.ntile(4).over(w_quality_by_site_installed_kw).alias("site_installed_kw_quartile"),
        F.ntile(3).over(w_all_by_voltage).alias("voltage_tercile"),
        F.max("voltage").over(w_all_by_voltage_moving_2).alias("voltage_max_3"),
        F.min("site_installed_kw").over(w_site_id).alias("site_id_min_site_installed_kw"),
        F.size(F.collect_set("quality").over(w_site_id)).alias("distinct_quality_in_site_id"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.lead("read_at").over(w_quality_site_id_by_voltage).alias("next_read_at"),
    )
)

scored = (
    with_trends
    .select(
        "*",
        F.round(F.col("kwh_adj") * 1.5, 2).alias("kwh_adj_adj"),
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix"),
        F.when(F.col("quality").isin("BAD", "EST"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
    )
)

windowed = (
    scored
    .select(
        "*",
        F.round(F.avg("rows_in_site_id").over(w_voltage_tercile_distinct_qua), 2).alias("rows_in_site_id_avg_1_prior"),
        F.sum("rows_in_site_id").over(w_voltage_tercile_by_next_read).alias("rows_in_site_id_sum_3_prior"),
        F.first("next_read_at", ignorenulls=True).over(w_all_by_site_installed_kw_qua).alias("first_known_next_read_at"),
    )
)

result = (
    windowed
    .select(
        "id",
        "voltage",
        "read_at_label",
        "kwh_adj",
        "site_installed_kw",
        "next3_read_at",
        "site_installed_kw_quartile",
        "voltage_tercile",
        "voltage_max_3",
        "site_id_min_site_installed_kw",
        "distinct_quality_in_site_id",
        "next_read_at",
        "quality_prefix",
        "rows_in_site_id_avg_1_prior",
        "first_known_next_read_at",
    )
    .orderBy(F.col("id").desc_nulls_first(), F.col("voltage_max_3").desc())
    .limit(20)
)
