from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_read_at = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_quality_group = Window.partitionBy("quality", "quality_group")
w_all_by_voltage_change = Window.orderBy("voltage_change", "id")
w_latest = Window.partitionBy("site_zone").orderBy(F.col("read_at").desc(), F.col("id").desc())
w_site_commissioned_on_month_b = Window.partitionBy(F.date_trunc("month", "site_commissioned_on")).orderBy("read_at")
w_site_commissioned_on_month_b_2 = Window.partitionBy(F.date_trunc("month", "site_commissioned_on")).orderBy("read_at", "id")

prepared_readings = (
    df_meter_readings
    .filter(F.col("quality").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("quality").isin("EST", "BAD"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
    )
)

with_trends = (
    prepared_readings
    .withColumn("voltage_change", F.col("voltage") - F.lag("voltage").over(w_quality_by_read_at))
    .withColumn("prev_kwh", F.lag("kwh", 1, 0).over(w_quality_by_read_at))
)

windowed = (
    with_trends
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "quality_group",
        F.count(F.lit(1)).over(w_quality_quality_group).alias("rows_in_quality_quality_group"),
        F.round(F.avg("prev_kwh").over(w_quality_quality_group), 2).alias("quality_quality_group_avg_prev_kwh"),
        F.lead("kwh", 3).over(w_all_by_voltage_change).alias("next3_kwh"),
    )
)

with_activity = (
    windowed
    .crossJoin(windowed.agg(F.max("kwh").alias("overall_max_kwh")))
    .filter(F.col("kwh") < F.col("overall_max_kwh"))
    .drop("overall_max_kwh")
)

joined_site = (
    with_activity.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.quality_group",
        "s.rows_in_quality_quality_group",
        "s.quality_quality_group_avg_prev_kwh",
        "s.next3_kwh",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone"),
    )
)

latest_per_key = (
    joined_site
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

windowed_2 = (
    latest_per_key
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        "rows_in_quality_quality_group",
        "quality_quality_group_avg_prev_kwh",
        "site_commissioned_on",
        F.round(F.cume_dist().over(w_site_commissioned_on_month_b), 3).alias("read_at_cume_dist"),
        F.lead("voltage").over(w_site_commissioned_on_month_b_2).alias("next_voltage"),
        F.dense_rank().over(w_site_commissioned_on_month_b).alias("read_at_dense_rank"),
        F.lag("site_commissioned_on", 3).over(w_site_commissioned_on_month_b_2).alias("prev3_site_commissioned_on"),
        (F.col("voltage") - F.lag("voltage").over(w_site_commissioned_on_month_b_2)).alias("voltage_change"),
        (F.col("next3_kwh") - F.lag("next3_kwh").over(w_site_commissioned_on_month_b_2)).alias("next3_kwh_change"),
    )
)

result = (
    windowed_2
    .filter(F.col("voltage").between(218.2, 241.8))
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        "rows_in_quality_quality_group",
        "quality_quality_group_avg_prev_kwh",
        "site_commissioned_on",
        "read_at_cume_dist",
        "next_voltage",
        "read_at_dense_rank",
        "prev3_site_commissioned_on",
        "next3_kwh_change",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
    )
    .orderBy(F.col("site_id").desc_nulls_first())
)
