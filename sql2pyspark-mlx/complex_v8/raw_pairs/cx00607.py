from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
    )
)

with_prev = base_readings.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_site_id_running))

annotated = (
    sessionized
    .select(
        "*",
        F.abs(F.col("voltage") - 225.2).alias("voltage_distance"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("prev_read_at")).alias("days_since_prev_read_at"),
    )
)

enriched_site = (
    annotated.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (~F.col("si.tariff").isin("TOU", "EV")), "inner")
    .filter(F.year(F.col("si.commissioned_on")) == 2024)
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.prev_read_at",
        "s.session_id",
        "s.voltage_distance",
        "s.days_since_prev_read_at",
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

result = (
    enriched_site
    .select(
        "id",
        "read_at",
        "voltage",
        "prev_read_at",
        "session_id",
        "voltage_distance",
        "days_since_prev_read_at",
        "site_zone",
        "site_commissioned_on",
    )
    .orderBy(F.col("read_at").desc())
)
