from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality")
w_site_zone_by_voltage = Window.partitionBy("site_zone").orderBy(F.col("voltage").desc(), "id")
w_quality_site_zone = Window.partitionBy("quality", "site_zone")
w_all_by_kwh_adj = Window.orderBy(F.col("kwh_adj").desc(), "id")
w_site_zone_by_site_commission = Window.partitionBy("site_zone").orderBy(F.col("site_commissioned_on").asc_nulls_last(), "id")
w_site_id_by_kwh_adj_ties = Window.partitionBy("site_id").orderBy(F.col("kwh_adj").desc())
w_all_by_site_installed_kw = Window.orderBy("site_installed_kw", "id")
w_site_id_by_kwh_adj = Window.partitionBy("site_id").orderBy(F.col("kwh_adj").desc(), "id")
w_all_by_site_installed_kw_ful = Window.orderBy("site_installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_readings = (
    df_meter_readings
    .filter((F.col("quality") == "EST") & (F.col("site_id") >= 5))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("kwh") * 1.5, 2).alias("kwh_adj"),
    )
)

with_parent_site = (
    scoped_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.zone").isNotNull())
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.kwh_adj",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

sequenced = (
    with_parent_site
    .select(
        "*",
        F.size(F.collect_set("site_zone").over(w_quality)).alias("distinct_site_zone_in_quality"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
        F.min("kwh_adj").over(w_quality).alias("quality_min_kwh_adj"),
        F.datediff(F.col("site_commissioned_on"), F.lag("site_commissioned_on").over(w_site_zone_by_voltage)).alias("days_since_prev_site_commissioned_on"),
        F.size(F.collect_set("site_id").over(w_quality_site_zone)).alias("distinct_site_id_in_quality_site_zone"),
        F.lag("site_installed_kw", 3).over(w_all_by_kwh_adj).alias("prev3_site_installed_kw"),
        F.round(F.col("site_installed_kw") / F.nullif(F.sum("site_installed_kw").over(w_quality_site_zone), F.lit(0)), 4).alias("site_installed_kw_share_of_quality_site_zone"),
        F.lead("read_at", 3).over(w_site_zone_by_site_commission).alias("next3_read_at"),
        F.round(F.cume_dist().over(w_site_id_by_kwh_adj_ties), 3).alias("kwh_adj_cume_dist"),
        F.dense_rank().over(w_all_by_site_installed_kw).alias("site_installed_kw_dense_rank"),
        F.lag("site_commissioned_on").over(w_site_id_by_kwh_adj).alias("prev_site_commissioned_on"),
        F.size(F.collect_set("site_id").over(w_quality)).alias("distinct_site_id_in_quality"),
        F.last("site_installed_kw").over(w_all_by_site_installed_kw_ful).alias("last_site_installed_kw"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "site_id",
        "read_at",
        "site_commissioned_on",
        "site_zone",
        "site_installed_kw",
        "distinct_site_zone_in_quality",
        "days_since_prev_site_commissioned_on",
        "distinct_site_id_in_quality_site_zone",
        "prev3_site_installed_kw",
        "next3_read_at",
        "kwh_adj_cume_dist",
        "site_installed_kw_dense_rank",
        "prev_site_commissioned_on",
        "distinct_site_id_in_quality",
        "last_site_installed_kw",
    )
    .orderBy("id", F.col("next3_read_at").desc())
)
