from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_by_voltage = Window.partitionBy(F.date_trunc("month", "read_at")).orderBy("voltage", "id")
w_all_by_voltage_adj_ties = Window.orderBy("voltage_adj")

scoped_readings = (
    df_meter_readings
    .filter((F.col("kwh").between(20.3, 83.8)) | (F.col("voltage") <= 235.0))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
    )
)

enriched = (
    scoped_readings
    .filter(F.col("site_id") >= 2)
    .withColumn("read_at_label", F.date_format(F.col("read_at"), "yyyy-MM"))
)

qualified = (
    enriched.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality") == "EST"), "left_anti")
)

tagged = (
    qualified
    .filter(F.col("kwh_per_voltage") <= 7.0)
    .select("*", F.round(F.col("voltage") * 0.9, 2).alias("voltage_adj"))
)

ranked = (
    tagged
    .withColumn("voltage_percent_rank", F.round(F.percent_rank().over(w_read_at_month_by_voltage), 3))
    .withColumn("voltage_adj_dense_rank", F.dense_rank().over(w_all_by_voltage_adj_ties))
)

exploded_detail_readings = (
    ranked.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh_per_voltage",
        "s.voltage_adj",
        "s.voltage_adj_dense_rank",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

combined_details_site = (
    exploded_detail_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh_per_voltage",
        "s.voltage_adj_dense_rank",
        "s.reading_id",
        "s.reading_read_at",
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

result = (
    combined_details_site
    .filter(F.col("kwh_per_voltage") <= 8.0)
    .select(
        "id",
        "read_at",
        "voltage_adj_dense_rank",
        "reading_id",
        "reading_read_at",
        "site_zone",
        "site_commissioned_on",
        "site_tariff",
    )
    .orderBy(F.col("site_zone").desc())
)
