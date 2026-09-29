from pyspark.sql import functions as F

est_by_site = (
    df_meter_readings
    .filter(F.col("quality") == "EST")
    .groupBy("site_id")
    .agg(
        F.count("*").alias("est_count"),
        F.round(F.avg("kwh"), 2).alias("est_avg_kwh"),
    )
)

ok_by_site = (
    df_meter_readings
    .filter(F.col("quality") == "OK")
    .groupBy("site_id")
    .agg(
        F.count("*").alias("ok_count"),
        F.round(F.avg("kwh"), 2).alias("ok_avg_kwh"),
    )
)

result = (
    est_by_site.alias("a")
    .join(ok_by_site.alias("b"), F.col("a.site_id") == F.col("b.site_id"), "full")
    .select(
        F.coalesce(F.col("a.site_id"), F.col("b.site_id")).alias("site_id"),
        F.coalesce(F.col("a.est_count"), F.lit(0)).alias("est_count"),
        F.col("a.est_avg_kwh"),
        F.coalesce(F.col("b.ok_count"), F.lit(0)).alias("ok_count"),
        F.col("b.ok_avg_kwh"),
        (F.coalesce(F.col("a.est_count"), F.lit(0)) - F.coalesce(F.col("b.ok_count"), F.lit(0))).alias("est_count_minus_ok_count"),
    )
    .orderBy("site_id")
)
