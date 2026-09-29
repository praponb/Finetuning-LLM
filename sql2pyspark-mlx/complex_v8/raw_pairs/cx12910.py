from pyspark.sql import functions as F

bad_by_site = (
    df_meter_readings
    .filter(F.col("quality") == "BAD")
    .groupBy("site_id")
    .agg(
        F.count("*").alias("bad_count"),
        F.round(F.avg("kwh"), 2).alias("bad_avg_kwh"),
    )
)

est_by_site = (
    df_meter_readings
    .filter(F.col("quality") == "EST")
    .groupBy("site_id")
    .agg(F.count("*").alias("est_count"), F.sum("kwh").alias("est_total_kwh"))
)

result = (
    bad_by_site.alias("a")
    .join(est_by_site.alias("b"), F.col("a.site_id") == F.col("b.site_id"), "full")
    .select(
        F.coalesce(F.col("a.site_id"), F.col("b.site_id")).alias("site_id"),
        F.coalesce(F.col("a.bad_count"), F.lit(0)).alias("bad_count"),
        F.coalesce(F.col("a.bad_avg_kwh"), F.lit(0)).alias("bad_avg_kwh"),
        F.coalesce(F.col("b.est_count"), F.lit(0)).alias("est_count"),
        F.coalesce(F.col("b.est_total_kwh"), F.lit(0)).alias("est_total_kwh"),
        (F.coalesce(F.col("a.bad_count"), F.lit(0)) - F.coalesce(F.col("b.est_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("site_id")
)
