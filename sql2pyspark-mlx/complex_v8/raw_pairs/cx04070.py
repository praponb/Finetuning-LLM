from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_zone_quality = Window.partitionBy("site_zone", "quality")
w_site_zone_by_site_installed = Window.partitionBy("site_zone").orderBy("site_installed_kw", "id")
w_quality_by_site_installed_kw = Window.partitionBy("quality").orderBy(F.col("site_installed_kw").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_site_zone = Window.partitionBy("quality", "site_zone")
w_site_id_quality_by_site_inst = Window.partitionBy("site_id", "quality").orderBy(F.col("site_installed_kw").desc(), "id")
w_all_by_site_installed_kw_ful = Window.orderBy(F.col("site_installed_kw").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_site_zone_by_site_in = Window.partitionBy("quality", "site_zone").orderBy("site_installed_kw", "id")
w_site_zone_by_site_installed_2 = Window.partitionBy("site_zone").orderBy("site_installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_meter_readings
    .filter((F.to_date(F.col("read_at")) < F.lit("2025-02-12").cast("date")) | (F.col("kwh").between(35.0, 106.5)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

joined_site = (
    scoped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.quality",
        "si.site_name",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone"),
    )
)

ranked = (
    joined_site
    .select(
        "id",
        "site_id",
        "site_name",
        "site_installed_kw",
        F.sum("site_installed_kw").over(w_site_zone_quality).alias("site_zone_quality_sum_site_installed_kw"),
        F.ntile(3).over(w_site_zone_by_site_installed).alias("site_installed_kw_tercile"),
        F.last("site_installed_kw", ignorenulls=True).over(w_quality_by_site_installed_kw).alias("last_known_site_installed_kw"),
        F.count(F.lit(1)).over(w_quality_site_zone).alias("rows_in_quality_site_zone"),
        F.max("site_installed_kw").over(w_site_zone_quality).alias("site_zone_quality_max_site_installed_kw"),
        F.dense_rank().over(w_site_id_quality_by_site_inst).alias("site_installed_kw_dense_rank"),
        F.last("site_installed_kw").over(w_all_by_site_installed_kw_ful).alias("last_site_installed_kw"),
        F.round(F.percent_rank().over(w_site_zone_by_site_installed), 3).alias("site_installed_kw_percent_rank"),
        F.round(F.percent_rank().over(w_quality_site_zone_by_site_in), 3).alias("site_installed_kw_percent_rank_2"),
        F.ntile(4).over(w_site_zone_by_site_installed).alias("site_installed_kw_quartile"),
        F.sum("site_installed_kw").over(w_site_zone_by_site_installed_2).alias("running_site_installed_kw"),
        F.size(F.collect_set("site_id").over(w_site_zone_quality)).alias("distinct_site_id_in_site_zone_quality"),
    )
)

top_per_group = (
    ranked
    .filter((F.col("site_installed_kw_dense_rank") == 1) & (F.col("site_installed_kw_percent_rank_2") < 0.9))
    .drop("site_installed_kw_dense_rank")
)

result = (
    top_per_group
    .select(
        "id",
        "site_name",
        "site_zone_quality_sum_site_installed_kw",
        "site_installed_kw_tercile",
        "last_known_site_installed_kw",
        "last_site_installed_kw",
        "site_installed_kw_percent_rank",
        "running_site_installed_kw",
        "distinct_site_id_in_site_zone_quality",
    )
    .orderBy(
        F.col("last_known_site_installed_kw").asc(),
        F.col("site_zone_quality_sum_site_installed_kw").asc_nulls_last(),
        "id",
    )
    .limit(5)
)
