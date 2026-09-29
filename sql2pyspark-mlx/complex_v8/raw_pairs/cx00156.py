from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_kwh = Window.orderBy(F.col("kwh").asc_nulls_last(), "id")
w_all_by_site_commissioned_on = Window.orderBy(F.col("site_commissioned_on").desc())
w_quality_site_id_by_kwh = Window.partitionBy("quality", "site_id").orderBy("kwh", "id")
w_quality_by_voltage_ties = Window.partitionBy("quality").orderBy(F.col("voltage").desc())
w_site_id = Window.partitionBy("site_id")
w_quality_by_voltage = Window.partitionBy("quality").orderBy(F.col("voltage").desc(), "id")
w_all_by_site_commissioned_on_2 = Window.orderBy(F.col("site_commissioned_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_quality = Window.partitionBy("site_id", "quality")
w_quality_by_voltage_moving_2 = Window.partitionBy("quality").orderBy(F.col("voltage").desc(), "id").rowsBetween(-2, Window.currentRow)
w_quality_site_id = Window.partitionBy("quality", "site_id")

prepared = (
    df_meter_readings
    .filter((F.col("voltage") > 222.3) & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_details = (
    prepared.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

metrics = (
    with_details
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        F.lead("voltage").over(w_all_by_kwh).alias("next_voltage"),
        F.rank().over(w_all_by_site_commissioned_on).alias("site_commissioned_on_rank"),
        F.lag("kwh", 3).over(w_quality_site_id_by_kwh).alias("prev3_kwh"),
        F.rank().over(w_quality_by_voltage_ties).alias("voltage_rank"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id), 2).alias("kwh_vs_site_id_avg"),
        (F.col("voltage") - F.lag("voltage").over(w_quality_by_voltage)).alias("voltage_change"),
        F.datediff(F.col("site_commissioned_on"), F.lag("site_commissioned_on").over(w_all_by_kwh)).alias("days_since_prev_site_commissioned_on"),
        F.size(F.collect_set("quality").over(w_site_id)).alias("distinct_quality_in_site_id"),
        F.first("voltage").over(w_all_by_site_commissioned_on_2).alias("first_voltage"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id_quality), F.lit(0)), 4).alias("kwh_share_of_site_id_quality"),
        F.round(F.avg("voltage").over(w_quality_by_voltage_moving_2), 2).alias("voltage_avg_3"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality_site_id), 2).alias("voltage_vs_quality_site_id_avg"),
    )
)

tagged = (
    metrics
    .filter(F.col("kwh_share_of_site_id_quality") <= 0.8)
    .withColumn("next_voltage_bucket", F.floor(F.col("next_voltage") / 5) * 5)
)

result = (
    tagged
    .filter(F.col("voltage_vs_quality_site_id_avg") <= 147.0)
    .select(
        "id",
        "next_voltage",
        "prev3_kwh",
        "voltage_rank",
        "kwh_vs_site_id_avg",
        "voltage_change",
        "first_voltage",
        "kwh_share_of_site_id_quality",
        "voltage_vs_quality_site_id_avg",
        F.least(F.col("kwh_vs_site_id_avg"), F.col("kwh")).alias("least_kwh_vs_site_id_avg_kwh"),
    )
)
