from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_readings = (
    df_meter_readings
    .filter(F.col("quality") == "OK")
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("kwh") * 1.1, 2).alias("kwh_adj"),
    )
)

with_prev = recent_readings.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_site_id_running))

with_details_site = (
    sessionized.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        "si.site_name",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

tagged = (
    with_details_site
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "prev_read_at",
        "is_new_session",
        "site_name",
        "site_installed_kw",
        "site_commissioned_on",
        F.round(F.col("site_installed_kw") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("site_installed_kw_per_kwh"),
        F.length(F.trim(F.col("site_name"))).alias("site_name_len"),
    )
)

result = (
    tagged
    .groupBy("read_at", "site_id")
    .pivot("quality", ["OK", "BAD"])
    .agg(F.sum("site_name_len").alias("total"), F.max("site_name_len").alias("max_v"))
    .orderBy("read_at", "site_id")
)
