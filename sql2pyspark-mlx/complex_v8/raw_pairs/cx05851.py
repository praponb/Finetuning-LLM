from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_kwh_ties = Window.partitionBy("site_id").orderBy("kwh")
w_site_id_by_kwh = Window.partitionBy("site_id").orderBy("kwh", "id")
w_site_id_by_kwh_full = Window.partitionBy("site_id").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_or_unknown_site_id = Window.partitionBy("quality_or_unknown", "site_id")

filtered_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
    )
)

sequenced = (
    filtered_readings
    .select(
        "id",
        "kwh",
        "voltage",
        "quality",
        "quality_or_unknown",
        "voltage_filled",
        F.rank().over(w_site_id_by_kwh_ties).alias("kwh_rank"),
        F.round(F.cume_dist().over(w_site_id_by_kwh), 3).alias("kwh_cume_dist"),
        F.nth_value("read_at", 2).over(w_site_id_by_kwh_full).alias("second_read_at"),
        F.sum("kwh").over(w_quality_or_unknown_site_id).alias("quality_or_unknown_site_id_sum_kwh"),
    )
)

annotated = (
    sequenced
    .filter(F.col("quality_or_unknown") == "UNKNOWN")
    .select(
        "id",
        "quality",
        "quality_or_unknown",
        "kwh_rank",
        "kwh_cume_dist",
        "second_read_at",
        "quality_or_unknown_site_id_sum_kwh",
        F.concat(F.col("quality"), F.lit("#"), F.col("kwh_rank").cast("string")).alias("quality_ref"),
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
        F.date_trunc("quarter", F.col("second_read_at")).alias("second_read_at_quarter"),
    )
)

top_per_group = (
    annotated
    .filter((F.col("kwh_rank") <= 2) & (F.col("second_read_at_quarter") >= F.lit("2025-01-13 09:00:00").cast("timestamp")))
    .drop("kwh_rank")
)

result = (
    top_per_group
    .filter(F.col("kwh_cume_dist") <= 0.6)
    .select(
        "id",
        "kwh_cume_dist",
        "second_read_at",
        "quality_or_unknown_site_id_sum_kwh",
        "quality_ref",
        "kwh_bucket",
        F.concat_ws(": ", F.col("quality_or_unknown"), F.col("quality")).alias("quality_or_unknown_quality_label"),
        F.round(F.col("kwh_bucket") / F.nullif(F.col("kwh_cume_dist"), F.lit(0)), 3).alias("kwh_bucket_per_kwh_cume_dist"),
    )
    .orderBy(
        F.col("kwh_bucket_per_kwh_cume_dist").desc(),
        F.col("quality_ref").desc(),
    )
)
