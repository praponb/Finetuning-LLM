from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at = Window.orderBy("read_at", "id")
w_quality_group_site_zone = Window.partitionBy("quality_group", "site_zone")
w_all_by_kwh_filled = Window.orderBy("kwh_filled", "id")
w_site_id_quality = Window.partitionBy("site_id", "quality")
w_site_id_quality_group_by_kwh = Window.partitionBy("site_id", "quality_group").orderBy("kwh", "id")
w_quality = Window.partitionBy("quality")
w_site_id_by_voltage = Window.partitionBy("site_id").orderBy("voltage", "id")
w_quality_group_by_voltage_tie = Window.partitionBy("quality_group").orderBy("voltage")
w_site_id_by_voltage_moving_2 = Window.partitionBy("site_id").orderBy("voltage", "id").rowsBetween(-2, Window.currentRow)

scoped_readings = (
    df_meter_readings
    .filter((F.col("quality") == "OK") | (F.col("kwh") >= 29.2))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
        F.when(F.col("quality").isin("EST", "OK"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
    )
)

with_parent = (
    scoped_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select("s.*", F.col("si.zone").alias("site_zone"))
)

windowed = (
    with_parent
    .select(
        "*",
        (F.col("voltage") - F.lag("voltage").over(w_all_by_read_at)).alias("voltage_change"),
        F.round(F.col("kwh_filled") / F.nullif(F.sum("kwh_filled").over(w_quality_group_site_zone), F.lit(0)), 4).alias("kwh_filled_share_of_quality_group_site_zone"),
        F.max("kwh_filled").over(w_quality_group_site_zone).alias("quality_group_site_zone_max_kwh_filled"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality_group_site_zone), 2).alias("kwh_vs_quality_group_site_zone_avg"),
        F.round(F.avg("kwh_filled").over(w_quality_group_site_zone), 2).alias("quality_group_site_zone_avg_kwh_filled"),
        F.lead("read_at", 2).over(w_all_by_kwh_filled).alias("next2_read_at"),
        F.max("kwh").over(w_quality_group_site_zone).alias("quality_group_site_zone_max_kwh"),
        F.round(F.percent_rank().over(w_all_by_kwh_filled), 3).alias("kwh_filled_percent_rank"),
    )
)

labeled = (
    windowed
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "kwh_filled",
        "quality_group",
        "site_zone",
        "quality_group_site_zone_max_kwh_filled",
        "kwh_vs_quality_group_site_zone_avg",
        "quality_group_site_zone_avg_kwh_filled",
        "quality_group_site_zone_max_kwh",
        F.date_format(F.col("next2_read_at"), "yyyy-MM-dd").alias("next2_read_at_label"),
    )
)

sequenced = (
    labeled
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "kwh_filled",
        "quality_group",
        "site_zone",
        "kwh_vs_quality_group_site_zone_avg",
        "quality_group_site_zone_avg_kwh_filled",
        "quality_group_site_zone_max_kwh",
        "next2_read_at_label",
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id_quality), 2).alias("voltage_vs_site_id_quality_avg"),
        F.rank().over(w_site_id_quality_group_by_kwh).alias("kwh_rank"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality), 2).alias("voltage_vs_quality_avg"),
        F.lead("kwh", 2).over(w_site_id_quality_group_by_kwh).alias("next2_kwh"),
        F.size(F.collect_set("site_zone").over(w_site_id_quality)).alias("distinct_site_zone_in_site_id_quality"),
        F.rank().over(w_site_id_by_voltage).alias("voltage_rank"),
        F.rank().over(w_quality_group_by_voltage_tie).alias("voltage_rank_2"),
        F.max("voltage").over(w_site_id_by_voltage_moving_2).alias("voltage_max_3"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "kwh_filled",
        "quality_group",
        "site_zone",
        "quality_group_site_zone_avg_kwh_filled",
        "quality_group_site_zone_max_kwh",
        "next2_read_at_label",
        "kwh_rank",
        "next2_kwh",
        "distinct_site_zone_in_site_id_quality",
        "voltage_rank",
        "voltage_rank_2",
        "voltage_max_3",
    )
    .orderBy(
        F.col("site_zone").asc(),
        F.col("distinct_site_zone_in_site_id_quality").desc_nulls_first(),
    )
)
