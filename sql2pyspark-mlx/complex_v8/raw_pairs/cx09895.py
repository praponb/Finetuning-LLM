from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("tariff", "zone").orderBy(F.col("installed_kw").desc(), F.col("id").desc())

filtered = (
    df_sites
    .filter(F.col("commissioned_on").isNotNull())
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.abs(F.col("installed_kw") - 284.0).alias("installed_kw_distance"),
        F.upper(F.trim(F.col("site_name"))).alias("site_name_clean"),
        F.concat(F.col("zone"), F.lit("#"), F.col("id").cast("string")).alias("zone_ref"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.max("read_at").alias("last_reading_read_at"))
)

with_counts = (
    filtered.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select("s.*", "m.last_reading_read_at")
)

unique_rows = (
    with_counts
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .select(
        "id",
        "commissioned_on",
        "site_name_clean",
        "zone_ref",
        "last_reading_read_at",
    )
    .orderBy(F.col("id").desc(), F.col("site_name_clean").desc())
)
