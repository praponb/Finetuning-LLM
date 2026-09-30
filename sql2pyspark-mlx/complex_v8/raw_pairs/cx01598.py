from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_dayofweek_read_at_to = Window.partitionBy("read_at_dayofweek", F.to_date("read_at"))

cleaned = (
    df_meter_readings
    .filter(F.col("site_id") <= 7)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        (F.col("read_at") + F.expr("INTERVAL 30 MINUTES")).alias("read_at_plus_30m"),
        F.when(F.col("kwh") >= 110.5, "HIGH").when(F.col("kwh") >= 29.2, "MEDIUM").otherwise("LOW").alias("kwh_tier"),
        F.dayofweek(F.col("read_at")).alias("read_at_dayofweek"),
    )
)

ranked = (
    cleaned
    .withColumn("read_at_dayofweek_read_at_to_date_sum_read_at_da", F.sum("read_at_dayofweek").over(w_read_at_dayofweek_read_at_to))
    .withColumn("read_at_dayofweek_read_at_to_date_avg_read_at_da", F.round(F.avg("read_at_dayofweek").over(w_read_at_dayofweek_read_at_to), 2))
)

with_details_site = (
    ranked.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.read_at_plus_30m",
        "s.kwh_tier",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

with_children = (
    with_details_site.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.read_at_plus_30m",
        "s.kwh_tier",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

result = (
    with_children
    .select(
        "id",
        "reading_id",
        "reading_kwh",
        "reading_voltage",
    )
    .unpivot(
        ids=["id", "reading_id"],
        values=["reading_kwh", "reading_voltage"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
)
