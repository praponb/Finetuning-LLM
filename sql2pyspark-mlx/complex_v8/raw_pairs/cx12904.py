from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality = Window.partitionBy("site_id", "quality")

cleaned = (
    df_meter_readings
    .filter(F.col("voltage") > 223.3)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

tagged = (
    cleaned
    .select(
        "*",
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

metrics = (
    tagged
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        F.size(F.collect_set("quality_or_unknown").over(w_site_id_quality)).alias("distinct_quality_or_unknown_in_site_id_quality"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id_quality), F.lit(0)), 4).alias("kwh_share_of_site_id_quality"),
    )
)

result = (
    metrics
    .groupingSets(
        [["quality", "site_id", "distinct_quality_or_unknown_in_site_id_quality"], ["quality"]],
        "quality", "site_id", "distinct_quality_or_unknown_in_site_id_quality",
    )
    .agg(F.countDistinct("quality").alias("distinct_qualitys"))
    .select(
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "site_id",
        "distinct_quality_or_unknown_in_site_id_quality",
        "distinct_qualitys",
    )
    .orderBy(
        F.col("quality").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
        F.col("distinct_quality_or_unknown_in_site_id_quality").asc_nulls_last(),
    )
)
