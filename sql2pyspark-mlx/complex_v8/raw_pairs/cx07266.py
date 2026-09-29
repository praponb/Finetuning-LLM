from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_voltage_prior_2 = Window.orderBy("voltage", "id").rowsBetween(-2, -1)
w_site_id = Window.partitionBy("site_id")

valid = (
    df_meter_readings
    .filter((F.col("voltage") <= 238.8) & (F.col("kwh") > 37.1))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("month", F.col("read_at")).alias("read_at_month"),
        F.quarter(F.col("read_at")).alias("read_at_quarter"),
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
    )
)

joined_site = (
    valid.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.year(F.col("si.commissioned_on")) == 2025), "left")
    .select("s.*", "si.site_name")
)

ranked = (
    joined_site
    .withColumn("voltage_avg_2_prior", F.round(F.avg("voltage").over(w_all_by_voltage_prior_2), 2))
    .withColumn("distinct_quality_in_site_id", F.size(F.collect_set("quality").over(w_site_id)))
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.count("*").alias("reading_count"))
)

with_counts = (
    ranked.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

combined_details = (
    with_counts.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.read_at_quarter",
        "s.site_name",
        "s.reading_count",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "site_id",
        "kwh",
        "read_at_quarter",
        "site_name",
        "site_commissioned_on",
    )
    .orderBy(F.col("site_commissioned_on").desc())
)
