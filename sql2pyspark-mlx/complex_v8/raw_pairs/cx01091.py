from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_quality_len = Window.orderBy("quality_len", "id")
w_quality = Window.partitionBy("quality")

scoped = (
    df_meter_readings
    .filter((F.col("kwh") > 45.0) & (F.col("quality").isin("BAD", "EST")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("read_at").alias("first_reading_read_at"),
        F.min("read_at").alias("first_reading_read_at_2"),
    )
)

profiled = (
    scoped.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.first_reading_read_at", "m.first_reading_read_at_2")
)

with_windows = (
    profiled
    .select(
        "id",
        "read_at",
        "voltage",
        "quality",
        "quality_len",
        "kwh_bucket",
        F.lag("kwh").over(w_all_by_quality_len).alias("prev_kwh"),
        F.size(F.collect_set("site_id").over(w_quality)).alias("distinct_site_id_in_quality"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_quality), F.lit(0)), 4).alias("voltage_share_of_quality"),
    )
)

quality_stats = (
    with_windows
    .groupBy("quality")
    .agg(F.avg("kwh_bucket").alias("avg_kwh_bucket"))
)

above_average = (
    with_windows.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.kwh_bucket") > F.col("g.avg_kwh_bucket"))
    .select(
        "s.*",
        "g.avg_kwh_bucket",
        F.round(F.col("s.kwh_bucket") - F.col("g.avg_kwh_bucket"), 2).alias("kwh_bucket_minus_avg"),
    )
)

result = (
    above_average
    .select("id", "voltage", "kwh_bucket_minus_avg")
    .unpivot(
        ids=["id"],
        values=["voltage", "kwh_bucket_minus_avg"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "measure")
)
