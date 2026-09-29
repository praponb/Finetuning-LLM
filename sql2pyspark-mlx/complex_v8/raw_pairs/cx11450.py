from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_dayofweek_read_at_da = Window.partitionBy("read_at_dayofweek", "read_at_dayofweek_2").orderBy(F.col("read_at_dayofweek").desc(), "id").rowsBetween(-2, Window.currentRow)
w_read_at_dayofweek_read_at_da_2 = Window.partitionBy("read_at_dayofweek", "read_at_dayofweek_2").orderBy(F.col("read_at_dayofweek").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_read_at_dayofweek = Window.partitionBy("site_id", "read_at_dayofweek")

scoped_readings = (
    df_meter_readings
    .filter(F.col("read_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.greatest(F.col("kwh"), F.col("voltage")).alias("greatest_kwh_voltage"),
    )
)

enriched = (
    scoped_readings
    .filter(F.col("read_at") >= F.lit("2025-01-31 10:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix"),
        F.dayofweek(F.col("read_at")).alias("read_at_dayofweek"),
        F.dayofweek(F.col("read_at")).alias("read_at_dayofweek_2"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.sum("read_at_dayofweek").over(w_read_at_dayofweek_read_at_da).alias("read_at_dayofweek_sum_3"),
        F.count(F.lit(1)).over(w_read_at_dayofweek_read_at_da_2).alias("running_count"),
        F.round(F.avg("read_at_dayofweek_2").over(w_site_id_read_at_dayofweek), 2).alias("site_id_read_at_dayofweek_avg_read_at_dayofweek_"),
        F.size(F.collect_set("quality").over(w_site_id_read_at_dayofweek)).alias("distinct_quality_in_site_id_read_at_dayofweek"),
    )
)

result = (
    metrics
    .groupBy("distinct_quality_in_site_id_read_at_dayofweek", "read_at_dayofweek")
    .pivot("quality", ["BAD", "EST", "OK"])
    .agg(F.min("site_id_read_at_dayofweek_avg_read_at_dayofweek_").alias("min_v"), F.sum("site_id_read_at_dayofweek_avg_read_at_dayofweek_").alias("sum_v"))
    .orderBy("distinct_quality_in_site_id_read_at_dayofweek", "read_at_dayofweek")
)
