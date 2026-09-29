from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_kwh_running = Window.orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_kwh = Window.orderBy("kwh", "id")
w_all_by_kwh_ties = Window.orderBy("kwh")
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_all_by_kwh_full = Window.orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_dedup = Window.partitionBy("quality").orderBy("quality_site_id_avg_kwh_bucket", "id")

recent = (
    df_meter_readings
    .filter((F.col("read_at") >= F.lit("2025-02-06 15:00:00").cast("timestamp")) | (F.col("voltage") > 220.2))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
    )
)

ranked = (
    recent
    .select(
        "id",
        "quality",
        "kwh_filled",
        "voltage_filled",
        F.last("kwh_bucket", ignorenulls=True).over(w_all_by_kwh_running).alias("last_known_kwh_bucket"),
        F.round(F.cume_dist().over(w_all_by_kwh), 3).alias("kwh_cume_dist"),
        F.round(F.cume_dist().over(w_all_by_kwh_ties), 3).alias("kwh_cume_dist_2"),
        F.round(F.avg("kwh_bucket").over(w_quality_site_id), 2).alias("quality_site_id_avg_kwh_bucket"),
        F.count(F.lit(1)).over(w_quality_site_id).alias("rows_in_quality_site_id"),
        F.first("kwh_bucket").over(w_all_by_kwh_full).alias("first_kwh_bucket"),
    )
)

annotated = (
    ranked
    .select(
        "id",
        "quality",
        "kwh_filled",
        "voltage_filled",
        "last_known_kwh_bucket",
        "kwh_cume_dist",
        "quality_site_id_avg_kwh_bucket",
        (F.col("kwh_cume_dist_2") - F.col("last_known_kwh_bucket")).alias("kwh_cume_dist_2_minus_last_known_kwh_bucket"),
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
    )
)

scored = annotated.select("*", F.initcap(F.trim(F.col("quality"))).alias("quality_clean"))

annotated_2 = (
    scored
    .select(
        "id",
        "quality",
        "voltage_filled",
        "last_known_kwh_bucket",
        "kwh_cume_dist",
        "quality_site_id_avg_kwh_bucket",
        "kwh_cume_dist_2_minus_last_known_kwh_bucket",
        "quality_prefix",
        "quality_clean",
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix_2"),
    )
)

unique_rows = (
    annotated_2
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

deduped = (
    unique_rows
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .select(
        "id",
        "voltage_filled",
        "kwh_cume_dist",
        "quality_site_id_avg_kwh_bucket",
        "kwh_cume_dist_2_minus_last_known_kwh_bucket",
        "quality_prefix",
        "quality_prefix_2",
    )
    .orderBy(
        F.col("quality_site_id_avg_kwh_bucket").desc_nulls_first(),
        "kwh_cume_dist_2_minus_last_known_kwh_bucket",
    )
)
