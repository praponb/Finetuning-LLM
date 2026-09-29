from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_read_at_month_by_vol = Window.partitionBy("quality", F.month("read_at")).orderBy(F.col("voltage").asc_nulls_last(), "id")
w_site_id_by_voltage_ties = Window.partitionBy("site_id").orderBy(F.col("voltage").desc())
w_site_id = Window.partitionBy("site_id")

scoped_readings = df_meter_readings.filter(F.col("voltage") < 231.7)

quality_stats = (
    scoped_readings
    .groupBy("quality")
    .agg(F.max("kwh").alias("max_kwh"))
)

above_average = (
    scoped_readings.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.kwh") == F.col("g.max_kwh"))
    .select(
        "s.*",
        "g.max_kwh",
        F.round(F.col("s.kwh") - F.col("g.max_kwh"), 2).alias("kwh_minus_max"),
    )
)

with_activity = (
    above_average.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.to_date(F.col("m.read_at")) < F.lit("2025-02-12").cast("date")) & (F.col("m.voltage") < 227.6), "left_anti")
    .filter(F.col("max_kwh") <= 78.0)
)

sequenced = (
    with_activity
    .select(
        "*",
        F.rank().over(w_quality_read_at_month_by_vol).alias("voltage_rank"),
        F.rank().over(w_site_id_by_voltage_ties).alias("voltage_rank_2"),
        F.round(F.avg("kwh_minus_max").over(w_site_id), 2).alias("site_id_avg_kwh_minus_max"),
    )
)

result = (
    sequenced
    .filter(F.col("kwh_minus_max") < 84.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "kwh_minus_max",
        "voltage_rank",
        "voltage_rank_2",
        "site_id_avg_kwh_minus_max",
        F.date_trunc("day", F.col("read_at")).alias("read_at_day"),
        F.round(F.col("kwh_minus_max") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("kwh_minus_max_per_kwh"),
    )
    .orderBy(F.col("read_at").desc(), F.col("kwh").asc(), "id")
    .limit(10)
)
