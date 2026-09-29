from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_site_id_site_zone_by_voltage = Window.partitionBy("site_id", "site_zone").orderBy("voltage", "id").rowsBetween(-1, -1)
w_quality_by_voltage_moving_6 = Window.partitionBy("quality").orderBy(F.col("voltage").desc(), "id").rowsBetween(-6, Window.currentRow)
w_read_at_to_date_by_voltage = Window.partitionBy(F.to_date("read_at")).orderBy("voltage", "id")
w_quality_site_zone = Window.partitionBy("quality", "site_zone")
w_quality_by_voltage = Window.partitionBy("quality").orderBy(F.col("voltage").desc(), "id")
w_site_id_by_voltage_moving_1 = Window.partitionBy("site_id").orderBy("voltage", "id").rowsBetween(-1, Window.currentRow)
w_all_by_read_at_ties = Window.orderBy(F.col("read_at").asc_nulls_last())
w_site_id_by_voltage_moving_3 = Window.partitionBy("site_id").orderBy("voltage", "id").rowsBetween(-3, Window.currentRow)
w_read_at_month_voltage_quarti = Window.partitionBy("read_at_month", "voltage_quartile")
w_distinct_site_id_in_quality = Window.partitionBy("distinct_site_id_in_quality_site_zone", "read_at_month").orderBy(F.col("read_at_year").desc(), "id").rowsBetween(-1, -1)
w_distinct_site_id_in_quality_2 = Window.partitionBy("distinct_site_id_in_quality_site_zone", "read_at_month")

recent = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

with_parent_site = (
    recent.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        F.col("si.zone").alias("site_zone"),
        "si.site_name",
    )
)

windowed = (
    with_parent_site
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.size(F.collect_set("quality").over(w_site_id)).alias("distinct_quality_in_site_id"),
        F.max("voltage").over(w_site_id_site_zone_by_voltage).alias("voltage_max_1_prior"),
        F.round(F.avg("voltage").over(w_quality_by_voltage_moving_6), 2).alias("voltage_avg_7"),
        F.round(F.cume_dist().over(w_read_at_to_date_by_voltage), 3).alias("voltage_cume_dist"),
        F.size(F.collect_set("site_id").over(w_quality_site_zone)).alias("distinct_site_id_in_quality_site_zone"),
        F.ntile(4).over(w_quality_by_voltage).alias("voltage_quartile"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_quality_site_zone), F.lit(0)), 4).alias("voltage_share_of_quality_site_zone"),
        F.count(F.lit(1)).over(w_quality_site_zone).alias("rows_in_quality_site_zone"),
        F.round(F.avg("voltage").over(w_site_id_by_voltage_moving_1), 2).alias("voltage_avg_2"),
        F.round(F.cume_dist().over(w_all_by_read_at_ties), 3).alias("read_at_cume_dist"),
        F.round(F.avg("voltage").over(w_site_id_by_voltage_moving_3), 2).alias("voltage_avg_4"),
    )
)

tagged = (
    windowed
    .filter(F.col("rows_in_quality_site_zone").between(3, 14))
    .select(
        "*",
        F.date_trunc("year", F.col("read_at")).alias("read_at_year"),
        F.month(F.col("read_at")).alias("read_at_month"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

sequenced = (
    tagged
    .select(
        "*",
        F.count(F.lit(1)).over(w_read_at_month_voltage_quarti).alias("rows_in_read_at_month_voltage_quartile"),
        F.max("voltage_avg_4").over(w_distinct_site_id_in_quality).alias("voltage_avg_4_max_1_prior"),
        F.round(F.col("voltage_avg_7") - F.avg("voltage_avg_7").over(w_distinct_site_id_in_quality_2), 2).alias("voltage_avg_7_vs_distinct_site_id_in_quality_sit"),
        F.round(F.col("read_at_cume_dist") - F.avg("read_at_cume_dist").over(w_distinct_site_id_in_quality_2), 2).alias("read_at_cume_dist_vs_distinct_site_id_in_quality"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "voltage",
        "quality",
        "distinct_quality_in_site_id",
        "voltage_max_1_prior",
        "voltage_cume_dist",
        "distinct_site_id_in_quality_site_zone",
        "voltage_quartile",
        "voltage_share_of_quality_site_zone",
        "rows_in_quality_site_zone",
        "voltage_avg_4",
        "read_at_year",
        "quality_slug",
        "voltage_avg_4_max_1_prior",
        "voltage_avg_7_vs_distinct_site_id_in_quality_sit",
        "read_at_cume_dist_vs_distinct_site_id_in_quality",
        F.concat_ws("-", F.col("quality"), F.col("quality_slug")).alias("quality_quality_slug_label"),
    )
    .orderBy(F.col("read_at_year").asc(), F.col("rows_in_quality_site_zone").desc())
)
