from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("quality", "site_id").orderBy("voltage", "id")

cleaned_readings = (
    df_meter_readings
    .filter(F.col("kwh") <= 96.8)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

combined_details_site = (
    cleaned_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.quality_clean",
        "si.site_name",
    )
)

latest = (
    combined_details_site
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

labeled = (
    latest
    .select(
        "*",
        (F.col("read_at") + F.expr("INTERVAL 1 DAYS")).alias("read_at_plus_1d"),
        F.concat_ws(": ", F.col("site_name"), F.col("quality")).alias("site_name_quality_label"),
    )
)

result = (
    labeled
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality_clean",
        "site_name",
        "site_name_quality_label",
        F.when(F.col("quality").isin("EST", "OK"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
    )
    .orderBy(F.col("id").asc())
)
