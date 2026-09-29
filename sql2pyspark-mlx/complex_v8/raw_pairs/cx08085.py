from pyspark.sql import functions as F

ok_by_site = (
    df_meter_readings
    .filter(F.col("quality") == "OK")
    .groupBy("site_id")
    .agg(F.count("*").alias("ok_count"), F.sum("kwh").alias("ok_total_kwh"))
)

est_by_site = (
    df_meter_readings
    .filter(F.col("quality") == "EST")
    .groupBy("site_id")
    .agg(F.count("*").alias("est_count"), F.sum("kwh").alias("est_total_kwh"))
)

result = (
    ok_by_site.alias("a")
    .join(est_by_site.alias("b"), F.col("a.site_id") == F.col("b.site_id"), "full")
    .select(
        F.coalesce(F.col("a.site_id"), F.col("b.site_id")).alias("site_id"),
        F.coalesce(F.col("a.ok_count"), F.lit(0)).alias("ok_count"),
        F.coalesce(F.col("a.ok_total_kwh"), F.lit(0)).alias("ok_total_kwh"),
        F.coalesce(F.col("b.est_count"), F.lit(0)).alias("est_count"),
        F.coalesce(F.col("b.est_total_kwh"), F.lit(0)).alias("est_total_kwh"),
        (F.coalesce(F.col("a.ok_count"), F.lit(0)) - F.coalesce(F.col("b.est_count"), F.lit(0))).alias("ok_count_minus_est_count"),
    )
    .orderBy("site_id")
)
