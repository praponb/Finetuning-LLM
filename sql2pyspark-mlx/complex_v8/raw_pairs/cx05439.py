from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id_by_voltage = Window.partitionBy("quality", "site_id").orderBy(F.col("voltage").desc(), "id")
w_all_by_kwh_filled = Window.orderBy("kwh_filled", "id")
w_all_by_kwh_filled_full = Window.orderBy("kwh_filled", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id = Window.partitionBy("site_id")

filtered = (
    df_meter_readings
    .filter((F.hour(F.col("read_at")).between(6, 9)) | (F.col("quality") == "EST"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
        F.upper(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "quality_prefix",
        "quality_clean",
        F.round(F.cume_dist().over(w_quality_site_id_by_voltage), 3).alias("voltage_cume_dist"),
        F.lag("kwh_filled", 3).over(w_all_by_kwh_filled).alias("prev3_kwh_filled"),
        F.ntile(4).over(w_all_by_kwh_filled).alias("kwh_filled_quartile"),
        F.nth_value("read_at", 2).over(w_all_by_kwh_filled_full).alias("second_read_at"),
        F.size(F.collect_set("quality").over(w_site_id)).alias("distinct_quality_in_site_id"),
    )
)

result = (
    with_windows
    .select("id", "voltage_cume_dist", "prev3_kwh_filled")
    .unpivot(
        ids=["id"],
        values=["voltage_cume_dist", "prev3_kwh_filled"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
