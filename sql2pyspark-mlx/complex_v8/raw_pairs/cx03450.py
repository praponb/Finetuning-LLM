from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at = Window.orderBy(F.col("read_at").asc_nulls_last(), "id")

valid = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
        F.abs(F.col("kwh") - 80.8).alias("kwh_distance"),
    )
)

filtered = (
    valid.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp")) & (F.col("m.voltage") > 224.8), "left_semi")
    .filter(F.col("voltage") < 237.5)
)

tagged = (
    filtered
    .select(
        "id",
        "read_at",
        "quality_ref",
        "quality_prefix",
        F.substring(F.trim(F.col("quality_prefix")), 1, 4).alias("quality_prefix_prefix"),
        F.upper(F.trim(F.col("quality_prefix"))).alias("quality_prefix_clean"),
    )
)

sequenced = (
    tagged
    .select(
        "*",
        F.dense_rank().over(w_all_by_read_at).alias("read_at_dense_rank"),
        F.row_number().over(w_all_by_read_at).alias("read_at_seq"),
        F.ntile(4).over(w_all_by_read_at).alias("read_at_quartile"),
        F.ntile(3).over(w_all_by_read_at).alias("read_at_tercile"),
    )
)

screened = (
    sequenced
    .crossJoin(sequenced.agg(F.max("read_at_dense_rank").alias("overall_max_read_at_dense_rank")))
    .filter(F.col("read_at_dense_rank") < F.col("overall_max_read_at_dense_rank"))
    .drop("overall_max_read_at_dense_rank")
)

with_activity = (
    screened
    .crossJoin(screened.agg(F.max("read_at_seq").alias("overall_max_read_at_seq")))
    .filter(F.col("read_at_seq") < F.col("overall_max_read_at_seq"))
    .drop("overall_max_read_at_seq")
)

result = (
    with_activity
    .select(
        "id",
        "quality_ref",
        "quality_prefix",
        "quality_prefix_prefix",
        "quality_prefix_clean",
        "read_at_dense_rank",
        "read_at_seq",
        "read_at_quartile",
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
    )
    .orderBy(
        F.col("read_at_label").desc_nulls_first(),
        F.col("quality_prefix_prefix").asc_nulls_last(),
    )
)
