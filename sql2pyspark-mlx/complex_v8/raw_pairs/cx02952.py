from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("site_id").orderBy(F.col("kwh").desc(), F.col("id").desc())

valid_readings = (
    df_meter_readings
    .filter((F.col("quality").isin("EST", "BAD")) & (F.hour(F.col("read_at")).between(9, 14)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

enriched_site = (
    valid_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select("s.*", F.col("si.zone").alias("site_zone"))
)

deduped = (
    enriched_site
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "quality_slug",
        F.month(F.col("read_at")).alias("read_at_month"),
    )
    .orderBy(F.col("quality_slug").desc(), F.col("kwh").desc())
)
