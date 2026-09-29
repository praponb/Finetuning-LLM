from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_all_by_read_at_prior_6 = Window.orderBy("read_at", "id").rowsBetween(-6, -1)
w_site_id_by_voltage_ties = Window.partitionBy("site_id").orderBy(F.col("voltage").asc_nulls_last())
w_all_by_read_at_running = Window.orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_voltage_ties_2 = Window.partitionBy("site_id").orderBy(F.col("voltage").desc())
w_read_at_year_quality_group_b = Window.partitionBy(F.year("read_at"), "quality_group").orderBy(F.col("kwh").desc(), "id")
w_quality_group = Window.partitionBy("quality_group")

recent = (
    df_meter_readings
    .filter((F.col("kwh") >= 52.5) & (F.col("site_id") >= 5))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.when(F.col("quality").isin("OK", "BAD"), "group_a").when(F.col("quality") == "EST", "group_b").otherwise("other").alias("quality_group"),
    )
)

with_parent = (
    recent.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.lower(F.col("si.site_name")).like("%ro%"))
    .select(
        "s.*",
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

ranked = (
    with_parent
    .select(
        "*",
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id), 2).alias("voltage_vs_site_id_avg"),
        F.sum("voltage").over(w_all_by_read_at_prior_6).alias("voltage_sum_6_prior"),
        F.size(F.collect_set("quality_group").over(w_site_id)).alias("distinct_quality_group_in_site_id"),
        F.dense_rank().over(w_site_id_by_voltage_ties).alias("voltage_dense_rank"),
        F.sum("voltage").over(w_all_by_read_at_running).alias("running_voltage"),
        F.rank().over(w_site_id_by_voltage_ties_2).alias("voltage_rank"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.ntile(3).over(w_read_at_year_quality_group_b).alias("kwh_tercile"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_quality_group), F.lit(0)), 4).alias("kwh_share_of_quality_group"),
    )
)

top_per_group = (
    ranked
    .filter(F.col("voltage_rank") <= 2)
    .drop("voltage_rank")
)

result = (
    top_per_group
    .filter(F.col("read_at") < F.lit("2025-02-12 08:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "site_name",
        "voltage_vs_site_id_avg",
        "voltage_sum_6_prior",
        "distinct_quality_group_in_site_id",
        "running_voltage",
        "rows_in_site_id",
        "kwh_tercile",
        "kwh_share_of_quality_group",
    )
)
