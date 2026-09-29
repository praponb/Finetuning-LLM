from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_zone = Window.partitionBy("quality", "site_zone")
w_site_zone_by_voltage_filled = Window.partitionBy("site_zone").orderBy("voltage_filled")
w_site_zone_by_voltage_filled_2 = Window.partitionBy("site_zone").orderBy("voltage_filled", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_zone_by_voltage_filled_3 = Window.partitionBy("site_zone").orderBy("voltage_filled", "id").rowsBetween(-2, -1)
w_site_zone_quality_by_kwh_tie = Window.partitionBy("site_zone", "quality").orderBy(F.col("kwh").desc_nulls_first())
w_site_zone_by_voltage_filled_4 = Window.partitionBy("site_zone").orderBy("voltage_filled", "id")
w_distinct_site_id_in_quality = Window.partitionBy("distinct_site_id_in_quality_site_zone").orderBy("quality_site_zone_avg_voltage", "id")
w_kwh_rank_by_kwh = Window.partitionBy("kwh_rank").orderBy(F.col("kwh").desc_nulls_first(), "id")
w_site_zone = Window.partitionBy("site_zone")
w_kwh_rank_by_kwh_ties = Window.partitionBy("kwh_rank").orderBy(F.col("kwh").desc_nulls_first())

scoped = (
    df_meter_readings
    .filter((F.col("kwh") >= 45.3) & (~F.col("quality").isin("OK", "EST")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.abs(F.col("kwh") - 53.6).alias("kwh_distance"),
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
    )
)

joined = (
    scoped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.kwh_distance",
        "s.voltage_filled",
        F.col("si.zone").alias("site_zone"),
    )
)

with_trends = (
    joined
    .select(
        "*",
        F.size(F.collect_set("site_id").over(w_quality_site_zone)).alias("distinct_site_id_in_quality_site_zone"),
        F.rank().over(w_site_zone_by_voltage_filled).alias("voltage_filled_rank"),
        F.last("kwh_distance").over(w_site_zone_by_voltage_filled_2).alias("last_kwh_distance"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality_site_zone), 2).alias("kwh_vs_quality_site_zone_avg"),
        F.max("voltage_filled").over(w_site_zone_by_voltage_filled_3).alias("voltage_filled_max_2_prior"),
        F.rank().over(w_site_zone_quality_by_kwh_tie).alias("kwh_rank"),
        F.round(F.avg("voltage").over(w_quality_site_zone), 2).alias("quality_site_zone_avg_voltage"),
        F.dense_rank().over(w_site_zone_by_voltage_filled_4).alias("voltage_filled_dense_rank"),
    )
)

ranked = (
    with_trends
    .select(
        "*",
        F.rank().over(w_distinct_site_id_in_quality).alias("quality_site_zone_avg_voltage_rank"),
        F.round(F.cume_dist().over(w_kwh_rank_by_kwh), 3).alias("kwh_cume_dist"),
        F.max("voltage_filled_rank").over(w_site_zone).alias("site_zone_max_voltage_filled_rank"),
        F.rank().over(w_kwh_rank_by_kwh_ties).alias("kwh_rank_2"),
        F.size(F.collect_set("site_id").over(w_site_zone)).alias("distinct_site_id_in_site_zone"),
        F.dense_rank().over(w_kwh_rank_by_kwh).alias("kwh_dense_rank"),
    )
)

result = (
    ranked
    .select(
        "id",
        "voltage",
        "quality",
        "kwh_distance",
        "voltage_filled",
        "site_zone",
        "distinct_site_id_in_quality_site_zone",
        "voltage_filled_rank",
        "last_kwh_distance",
        "kwh_vs_quality_site_zone_avg",
        "voltage_filled_max_2_prior",
        "kwh_rank",
        "quality_site_zone_avg_voltage",
        "distinct_site_id_in_site_zone",
        "kwh_dense_rank",
    )
    .orderBy(
        F.col("kwh_rank").asc_nulls_last(),
        F.col("voltage_filled_max_2_prior").asc(),
        "id",
    )
    .limit(20)
)
