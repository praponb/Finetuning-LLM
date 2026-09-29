from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_site_zone_is_not_null_qual = Window.partitionBy("n_site_zone_is_not_null", "quality_or_unknown")
w_cnt_n_site_zone_is_not_null = Window.partitionBy("cnt", "n_site_zone_is_not_null").orderBy("kwh_to_voltage_ratio", "site_commissioned_on_month", "quality_or_unknown", "site_zone").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_meter_readings
    .filter(F.month(F.col("read_at")).isin(2, 3))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.when(F.col("quality").isin("BAD", "EST"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

joined_site = (
    recent.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality_slug",
        "s.quality_or_unknown",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone"),
    )
)

per_group = (
    joined_site
    .filter(F.col("voltage") <= 236.7)
    .groupBy(
        F.date_trunc("month", "site_commissioned_on").alias("site_commissioned_on_month"),
        "quality_or_unknown",
        "site_zone",
    )
    .agg(
        F.count(F.when(F.col("site_zone").isNotNull(), 1)).alias("n_site_zone_is_not_null"),
        F.round(F.sum("kwh") / F.nullif(F.sum("voltage"), F.lit(0)), 3).alias("kwh_to_voltage_ratio"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 2)
)

tagged = (
    per_group
    .filter(F.col("site_zone") == "Z2")
    .select(
        "*",
        F.round(F.col("kwh_to_voltage_ratio") * 1.1, 2).alias("kwh_to_voltage_ratio_adj"),
        F.when(F.col("site_zone").isin("Z3", "Z2"), "group_a").when(F.col("site_zone") == "Z4", "group_b").otherwise("other").alias("site_zone_group"),
        F.lower(F.trim(F.col("quality_or_unknown"))).alias("quality_or_unknown_clean"),
    )
)

sequenced = (
    tagged
    .select(
        "*",
        F.size(F.collect_set("site_zone_group").over(w_n_site_zone_is_not_null_qual)).alias("distinct_site_zone_group_in_n_site_zone_is_not_n"),
        F.round(F.avg("cnt").over(w_n_site_zone_is_not_null_qual), 2).alias("n_site_zone_is_not_null_quality_or_unknown_avg_c"),
        F.round(F.col("n_site_zone_is_not_null") / F.nullif(F.sum("n_site_zone_is_not_null").over(w_n_site_zone_is_not_null_qual), F.lit(0)), 4).alias("n_site_zone_is_not_null_share_of_n_site_zone_is_"),
        F.nth_value("n_site_zone_is_not_null", 2).over(w_cnt_n_site_zone_is_not_null).alias("second_n_site_zone_is_not_null"),
    )
)

derived = (
    sequenced
    .select(
        "site_commissioned_on_month",
        "quality_or_unknown",
        "site_zone",
        "n_site_zone_is_not_null",
        "kwh_to_voltage_ratio",
        "cnt",
        "quality_or_unknown_clean",
        "distinct_site_zone_group_in_n_site_zone_is_not_n",
        "n_site_zone_is_not_null_quality_or_unknown_avg_c",
        "n_site_zone_is_not_null_share_of_n_site_zone_is_",
        (F.col("cnt") + F.col("kwh_to_voltage_ratio_adj")).alias("cnt_plus_kwh_to_voltage_ratio_adj"),
        (F.floor(F.col("kwh_to_voltage_ratio_adj") / 2) * 2).alias("kwh_to_voltage_ratio_adj_bucket"),
    )
)

tagged_2 = (
    derived
    .select(
        "site_commissioned_on_month",
        "quality_or_unknown",
        "site_zone",
        "n_site_zone_is_not_null",
        "kwh_to_voltage_ratio",
        "cnt",
        "quality_or_unknown_clean",
        "distinct_site_zone_group_in_n_site_zone_is_not_n",
        "n_site_zone_is_not_null_quality_or_unknown_avg_c",
        "n_site_zone_is_not_null_share_of_n_site_zone_is_",
        "kwh_to_voltage_ratio_adj_bucket",
        (F.floor(F.col("cnt") / 2) * 2).alias("cnt_bucket"),
    )
)

result = (
    tagged_2
    .filter(F.length(F.trim(F.col("quality_or_unknown_clean"))) >= 4)
    .select(
        "site_commissioned_on_month",
        "quality_or_unknown",
        "site_zone",
        "n_site_zone_is_not_null",
        "cnt",
        "quality_or_unknown_clean",
        "n_site_zone_is_not_null_quality_or_unknown_avg_c",
        "n_site_zone_is_not_null_share_of_n_site_zone_is_",
        "kwh_to_voltage_ratio_adj_bucket",
        (F.col("kwh_to_voltage_ratio") + F.col("cnt_bucket")).alias("kwh_to_voltage_ratio_plus_cnt_bucket"),
        F.coalesce(F.col("site_zone"), F.lit("UNKNOWN")).alias("site_zone_or_unknown"),
    )
)
