from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_quality_by_max_kwh = Window.partitionBy("quality").orderBy("max_kwh", "id")

cleaned_readings = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) | (F.col("kwh") <= 113.1))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

quality_stats = (
    cleaned_readings
    .groupBy("quality")
    .agg(F.max("kwh").alias("max_kwh"))
)

above_average = (
    cleaned_readings.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.kwh") == F.col("g.max_kwh"))
    .select(
        "s.*",
        "g.max_kwh",
        F.round(F.col("s.kwh") - F.col("g.max_kwh"), 2).alias("kwh_minus_max"),
    )
)

windowed = (
    above_average
    .withColumn("site_id_avg_kwh_minus_max", F.round(F.avg("kwh_minus_max").over(w_site_id), 2))
    .withColumn("max_kwh_rank", F.rank().over(w_quality_by_max_kwh))
)

result = (
    windowed
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "kwh_minus_max",
        "max_kwh_rank",
        (F.col("kwh") - F.col("max_kwh")).alias("kwh_minus_max_kwh"),
    )
    .orderBy(F.col("quality").desc(), F.col("kwh").desc())
)
