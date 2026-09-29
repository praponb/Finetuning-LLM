from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_zone = Window.partitionBy("quality", "site_zone")
w_site_id = Window.partitionBy("site_id")
w_site_id_by_kwh = Window.partitionBy("site_id").orderBy(F.col("kwh").desc(), "id")
w_site_zone_distinct_site_zone = Window.partitionBy("site_zone", "distinct_site_zone_in_site_id")
w_site_zone_by_kwh_percent_ran = Window.partitionBy("site_zone").orderBy("kwh_percent_rank", "id")
w_site_id_distinct_site_zone_i = Window.partitionBy("site_id", "distinct_site_zone_in_site_id").orderBy("read_at", "id")
w_site_id_by_kwh_vs_site_id_av = Window.partitionBy("site_id").orderBy("kwh_vs_site_id_avg")

prepared = (
    df_meter_readings
    .filter(F.col("kwh") <= 90.6)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

joined_site = (
    prepared.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.zone") == "Z3")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.quality_ref",
        "s.quality_slug",
        F.col("si.zone").alias("site_zone"),
    )
)

ranked = (
    joined_site
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "quality_ref",
        "site_zone",
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality_site_zone), 2).alias("kwh_vs_quality_site_zone_avg"),
        F.size(F.collect_set("site_zone").over(w_site_id)).alias("distinct_site_zone_in_site_id"),
        F.round(F.percent_rank().over(w_site_id_by_kwh), 3).alias("kwh_percent_rank"),
        F.count(F.lit(1)).over(w_quality_site_zone).alias("rows_in_quality_site_zone"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_site_id_by_kwh))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id), 2).alias("kwh_vs_site_id_avg"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
    )
)

with_windows = (
    ranked
    .select(
        "*",
        F.size(F.collect_set("site_id").over(w_site_zone_distinct_site_zone)).alias("distinct_site_id_in_site_zone_distinct_site_zone"),
        F.count(F.lit(1)).over(w_site_zone_distinct_site_zone).alias("rows_in_site_zone_distinct_site_zone_in_site_id"),
        F.lead("read_at").over(w_site_zone_by_kwh_percent_ran).alias("next_read_at"),
        F.lag("kwh_percent_rank").over(w_site_id_distinct_site_zone_i).alias("prev_kwh_percent_rank"),
        F.round(F.percent_rank().over(w_site_id_by_kwh_vs_site_id_av), 3).alias("kwh_vs_site_id_avg_percent_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "site_zone",
        "kwh_vs_quality_site_zone_avg",
        "distinct_site_zone_in_site_id",
        "kwh_percent_rank",
        "rows_in_quality_site_zone",
        "kwh_vs_site_id_avg",
        "rows_in_site_id",
        "distinct_site_id_in_site_zone_distinct_site_zone",
        "rows_in_site_zone_distinct_site_zone_in_site_id",
        "prev_kwh_percent_rank",
        "kwh_vs_site_id_avg_percent_rank",
    )
    .orderBy("rows_in_site_id", F.col("kwh_vs_quality_site_zone_avg").desc(), "id")
    .limit(20)
)
