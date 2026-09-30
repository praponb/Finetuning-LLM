from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_commissioned_on = Window.partitionBy("quality", F.year("site_commissioned_on"))
w_quality_site_id_by_kwh_dista = Window.partitionBy("quality", "site_id").orderBy(F.col("kwh_distance").desc(), "id")
w_site_id = Window.partitionBy("site_id")
w_quality_site_id_by_kwh_dista_2 = Window.partitionBy("quality", "site_id").orderBy(F.col("kwh_distance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_by_kwh_adj = Window.partitionBy("quality").orderBy(F.col("kwh_adj").desc(), "id")
w_site_id_site_commissioned_on = Window.partitionBy("site_id", F.month("site_commissioned_on")).orderBy("site_commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_readings = (
    df_meter_readings
    .filter(F.col("voltage") > 221.4)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.round(F.col("kwh") * 0.25, 2).alias("kwh_adj"),
        F.abs(F.col("kwh") - 68.1).alias("kwh_distance"),
    )
)

enriched = (
    filtered_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.lower(F.col("si.site_name")).like("%m %")), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.kwh_adj",
        "s.kwh_distance",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

windowed = (
    enriched
    .select(
        "*",
        F.size(F.collect_set("site_id").over(w_quality_site_commissioned_on)).alias("distinct_site_id_in_quality_site_commissioned_on"),
        F.lag("kwh_adj", 3).over(w_quality_site_id_by_kwh_dista).alias("prev3_kwh_adj"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_quality_site_commissioned_on), F.lit(0)), 4).alias("kwh_share_of_quality_site_commissioned_on_year"),
        F.round(F.col("kwh_distance") - F.avg("kwh_distance").over(w_site_id), 2).alias("kwh_distance_vs_site_id_avg"),
        F.ntile(5).over(w_quality_site_id_by_kwh_dista).alias("kwh_distance_quintile"),
        F.first("site_commissioned_on").over(w_quality_site_id_by_kwh_dista_2).alias("first_site_commissioned_on"),
        F.rank().over(w_quality_by_kwh_adj).alias("kwh_adj_rank"),
        F.lag("kwh_adj").over(w_quality_by_kwh_adj).alias("prev_kwh_adj"),
        F.max("kwh_distance").over(w_site_id_site_commissioned_on).alias("max_kwh_distance_so_far"),
    )
)

result = (
    windowed
    .filter(F.col("prev_kwh_adj") < 117.0)
    .select(
        "id",
        "kwh",
        "quality",
        "kwh_adj",
        "kwh_distance",
        "site_commissioned_on",
        "distinct_site_id_in_quality_site_commissioned_on",
        "prev3_kwh_adj",
        "kwh_share_of_quality_site_commissioned_on_year",
        "kwh_distance_quintile",
        "max_kwh_distance_so_far",
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("site_commissioned_on")).alias("days_since_site_commissioned_on"),
    )
    .orderBy(
        F.col("kwh_adj").desc(),
        F.col("distinct_site_id_in_quality_site_commissioned_on").asc(),
    )
)
