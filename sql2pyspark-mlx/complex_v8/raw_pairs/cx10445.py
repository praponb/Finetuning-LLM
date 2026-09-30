from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality")
w_all_by_read_at = Window.orderBy("read_at", "id")
w_reading_count_by_greatest_vo = Window.partitionBy("reading_count").orderBy("greatest_voltage_kwh", "id")
w_dedup = Window.partitionBy("quality").orderBy(F.col("rows_in_quality").desc(), F.col("id").desc())

valid = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("voltage") * 1.1, 2).alias("voltage_adj"),
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality") == "BAD", 1)).alias("reading_quality_count"),
        F.count("*").alias("reading_count"),
    )
)

with_stats = (
    valid.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

labeled = (
    with_stats
    .select(
        "id",
        "read_at",
        "voltage",
        "quality",
        "voltage_adj",
        "reading_quality_count",
        "reading_count",
        F.greatest(F.col("voltage"), F.col("kwh")).alias("greatest_voltage_kwh"),
    )
)

sequenced = (
    labeled
    .select(
        "id",
        "read_at",
        "quality",
        "voltage_adj",
        "greatest_voltage_kwh",
        F.max("voltage_adj").over(w_quality).alias("quality_max_voltage_adj"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
        F.lead("greatest_voltage_kwh").over(w_all_by_read_at).alias("next_greatest_voltage_kwh"),
        F.round(F.percent_rank().over(w_reading_count_by_greatest_vo), 3).alias("greatest_voltage_kwh_percent_rank"),
    )
)

latest = (
    sequenced
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_activity = (
    latest
    .crossJoin(latest.agg(F.avg("next_greatest_voltage_kwh").alias("overall_avg_next_greatest_voltage_kwh")))
    .filter(F.col("next_greatest_voltage_kwh") > F.col("overall_avg_next_greatest_voltage_kwh"))
    .drop("overall_avg_next_greatest_voltage_kwh")
)

result = (
    with_activity
    .filter(F.col("next_greatest_voltage_kwh") >= 73.5)
    .select(
        "id",
        "read_at",
        "quality",
        "voltage_adj",
        "greatest_voltage_kwh",
        "quality_max_voltage_adj",
        "rows_in_quality",
        "next_greatest_voltage_kwh",
        "greatest_voltage_kwh_percent_rank",
        F.coalesce(F.col("quality_max_voltage_adj"), F.lit(0.0)).alias("quality_max_voltage_adj_filled"),
    )
)
