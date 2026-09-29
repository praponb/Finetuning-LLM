from pyspark.sql import functions as F

scoped_readings = (
    df_meter_readings
    .filter((F.hour(F.col("read_at")).between(6, 10)) | (F.col("kwh") < 109.0))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

scored = (
    scoped_readings
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        "quality_len",
        F.round(F.col("quality_len") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("quality_len_per_voltage"),
    )
)

result = (
    scored
    .cube("quality", "quality_len")
    .agg(
        F.grouping_id().alias("gid"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
    )
)
