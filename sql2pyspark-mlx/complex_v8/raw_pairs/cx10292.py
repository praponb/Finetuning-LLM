from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("site_zone").orderBy(F.col("median_kwh").desc(), F.col("site_id").desc(), F.col("read_at_day").desc())

prepared_readings = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
    )
)

rollup_base = (
    prepared_readings
    .groupBy("site_id", F.to_date("read_at").alias("read_at_day"))
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("voltage"), 2).alias("avg_voltage"),
        F.max("read_at").alias("last_read_at"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
        F.percentile_approx("kwh", 0.5).alias("median_kwh"),
    )
)

enriched_site = (
    rollup_base.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.site_id",
        "s.read_at_day",
        "s.row_count",
        "s.avg_voltage",
        "s.last_read_at",
        "s.distinct_site_ids",
        "s.median_kwh",
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

unique_rows = (
    enriched_site
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .groupBy("distinct_site_ids", "row_count")
    .pivot("site_zone", ["Z4", "Z3", "Z2"])
    .agg(F.count("site_installed_kw"))
)
