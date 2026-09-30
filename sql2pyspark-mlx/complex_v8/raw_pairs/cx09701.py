from pyspark.sql import functions as F

cleaned_readings = (
    df_meter_readings
    .filter(F.col("site_id") <= 5)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

tagged = (
    cleaned_readings
    .select(
        "*",
        F.substring(F.trim(F.col("quality_clean")), 1, 4).alias("quality_clean_prefix"),
        F.round(F.col("kwh") * 0.25, 2).alias("kwh_adj"),
    )
)

result = (
    tagged
    .select(
        "id",
        "voltage",
        "quality",
        "quality_clean",
        "quality_clean_prefix",
        "kwh_adj",
    )
)
