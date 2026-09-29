from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_latest = Window.partitionBy("site_zone", "site_id").orderBy("is_new_session", "id")

cleaned_readings = df_meter_readings

with_prev = cleaned_readings.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_site_id_running))

with_details_site = (
    sessionized.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.zone") == "Z4")
    .select(
        "s.id",
        "s.site_id",
        "s.quality",
        "s.prev_read_at",
        "s.is_new_session",
        "s.session_no",
        F.col("si.zone").alias("site_zone"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

deduped = (
    with_details_site
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .cube("is_new_session", "quality")
    .agg(F.max("session_no").alias("max_session_no"))
    .orderBy(
        F.col("is_new_session").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
    )
)
