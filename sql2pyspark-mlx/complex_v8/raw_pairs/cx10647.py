from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_installed_kw_tier_tariff_by = Window.partitionBy("installed_kw_tier", "tariff").orderBy(F.col("avg_reading_kwh").desc(), "id")

prepared = (
    df_sites
    .filter((F.col("tariff").isin("TOU", "EV")) | (F.col("commissioned_on") < F.lit("2025-05-07").cast("date")))
    .select(
        "id",
        "site_name",
        "tariff",
        "installed_kw",
        F.when(F.col("installed_kw") >= 386.6, "A").when(F.col("installed_kw") >= 141.6, "B").otherwise("C").alias("installed_kw_tier"),
        F.dayofweek(F.col("commissioned_on")).alias("commissioned_on_dayofweek"),
    )
)

derived = (
    prepared
    .filter(F.col("commissioned_on_dayofweek") > 2)
    .withColumn("installed_kw_tier_slug", F.lower(F.regexp_replace(F.trim(F.col("installed_kw_tier")), " ", "_")))
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.round(F.avg("kwh"), 2).alias("avg_reading_kwh"),
        F.max("read_at").alias("last_reading_read_at"),
        F.min("voltage").alias("min_reading_voltage"),
    )
)

summarized_rows = (
    derived.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "m.avg_reading_kwh",
        "m.last_reading_read_at",
        "m.min_reading_voltage",
    )
)

sequenced = (
    summarized_rows
    .withColumn("rn", F.row_number().over(w_installed_kw_tier_tariff_by))
    .withColumn("avg_reading_kwh_percent_rank", F.round(F.percent_rank().over(w_installed_kw_tier_tariff_by), 3))
)

result = (
    sequenced
    .groupBy("rn")
    .pivot("tariff", ["TOU", "FLAT", "EV"])
    .agg(F.avg("commissioned_on_dayofweek"))
    .orderBy("rn")
)
