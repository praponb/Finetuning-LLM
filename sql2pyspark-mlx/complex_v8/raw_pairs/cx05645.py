from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_site_id_by_read_at = Window.partitionBy("site_id").orderBy(F.col("read_at").desc(), "id")
w_site_id_by_read_at_full = Window.partitionBy("site_id").orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_meter_readings
    .filter((F.col("quality") != "BAD") & (F.col("read_at") >= F.lit("2025-01-25 09:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

ranked = (
    valid
    .select(
        "id",
        "read_at",
        "voltage",
        "quality",
        F.sum("days_since_read_at").over(w_site_id).alias("site_id_sum_days_since_read_at"),
        F.lead("days_since_read_at").over(w_site_id_by_read_at).alias("next_days_since_read_at"),
        F.last("voltage").over(w_site_id_by_read_at_full).alias("last_voltage"),
        F.row_number().over(w_site_id_by_read_at).alias("rn"),
        F.min("days_since_read_at").over(w_site_id).alias("site_id_min_days_since_read_at"),
    )
)

result = (
    ranked
    .rollup("rn", "quality")
    .agg(
        F.grouping("rn").alias("is_total_rn"),
        F.percentile_approx("site_id_min_days_since_read_at", 0.5).alias("median_site_id_min_days_since_read_at"),
        F.count(F.when(F.col("quality").isNotNull(), 1)).alias("n_quality_is_not_null"),
        F.min("next_days_since_read_at").alias("min_next_days_since_read_at"),
    )
)
