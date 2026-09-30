from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality")
w_site_id_by_kwh_running = Window.partitionBy("site_id").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_latest = Window.partitionBy("quality").orderBy(F.col("voltage").desc(), F.col("id").desc())
w_latest_2 = Window.partitionBy("quality").orderBy(F.col("first_known_kwh").desc(), F.col("id").desc())

valid = (
    df_meter_readings
    .filter((F.col("voltage") > 221.8) & (F.month(F.col("read_at")).isin(1, 2)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
        F.when(F.col("voltage") >= 235.0, "large").when(F.col("voltage") >= 223.3, "medium").otherwise("small").alias("voltage_tier"),
    )
)

ranked = (
    valid
    .withColumn("quality_max_kwh", F.max("kwh").over(w_quality))
    .withColumn("first_known_kwh", F.first("kwh", ignorenulls=True).over(w_site_id_by_kwh_running))
)

latest = (
    ranked
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("read_at").alias("first_reading_read_at"),
        F.min("read_at").alias("first_reading_read_at_2"),
    )
)

profiled = (
    latest.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.first_reading_read_at", "m.first_reading_read_at_2")
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("kwh").alias("max_reading_kwh"),
        F.max("read_at").alias("last_reading_read_at"),
    )
)

summarized_rows = (
    profiled.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.max_reading_kwh", "m.last_reading_read_at")
)

deduped = (
    summarized_rows
    .withColumn("recency_rank", F.row_number().over(w_latest_2))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

enriched_site = (
    deduped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.site_name").isNotNull())
    .select(
        "s.*",
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

result = (
    enriched_site
    .groupBy("read_at")
    .pivot("quality", ["EST", "BAD"])
    .agg(F.max("quality_max_kwh"))
)
