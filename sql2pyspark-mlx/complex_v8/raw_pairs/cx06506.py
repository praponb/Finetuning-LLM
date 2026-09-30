from pyspark.sql import functions as F

recent = (
    df_meter_readings
    .filter((F.col("voltage").between(215.6, 232.0)) & (F.hour(F.col("read_at")).between(10, 14)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.dayofweek(F.col("read_at")).alias("read_at_dayofweek"),
    )
)

labeled = (
    recent
    .select(
        "id",
        "site_id",
        "voltage",
        "quality_slug",
        F.length(F.trim(F.col("quality_slug"))).alias("quality_slug_len"),
        F.lower(F.regexp_replace(F.trim(F.col("quality_slug")), " ", "_")).alias("quality_slug_slug"),
    )
)

result = (
    labeled
    .select(
        "id",
        "site_id",
        "voltage",
        "quality_slug",
        F.greatest(F.col("quality_slug_len"), F.col("voltage")).alias("greatest_quality_slug_len_voltage"),
    )
)
