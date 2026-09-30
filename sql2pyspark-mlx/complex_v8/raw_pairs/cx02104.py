from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("site_id").orderBy(F.col("voltage").desc(), F.col("id").desc())

scoped = (
    df_meter_readings
    .filter(F.col("site_id") >= 2)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
    )
)

scored = (
    scoped
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        F.date_trunc("year", F.col("read_at")).alias("read_at_year"),
    )
)

latest = (
    scored
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

stats = (
    latest
    .groupBy("site_id")
    .agg(
        F.round(F.sum("voltage") / F.nullif(F.sum("kwh"), F.lit(0)), 3).alias("voltage_to_kwh_ratio"),
        F.min("read_at_year").alias("first_read_at_year"),
        F.percentile_approx("kwh", 0.5).alias("median_kwh"),
        F.max("read_at_year").alias("last_read_at_year"),
    )
)

with_activity = (
    stats.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") <= 229.9), "left_anti")
)

result = (
    with_activity
    .filter(F.to_date(F.col("last_read_at_year")) < F.lit("2025-01-21").cast("date"))
    .select(
        "site_id",
        "voltage_to_kwh_ratio",
        "first_read_at_year",
        "last_read_at_year",
        F.round(F.col("median_kwh") / F.nullif(F.col("voltage_to_kwh_ratio"), F.lit(0)), 3).alias("median_kwh_per_voltage_to_kwh_ratio"),
        F.round(F.col("voltage_to_kwh_ratio") * 0.9, 2).alias("voltage_to_kwh_ratio_adj"),
    )
    .orderBy(
        F.col("site_id").desc_nulls_first(),
        F.col("voltage_to_kwh_ratio").desc_nulls_first(),
    )
)
