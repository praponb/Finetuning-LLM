from pyspark.sql import functions as F

est_monthly = (
    df_meter_readings
    .filter(F.col("quality") == "EST")
    .groupBy(F.date_trunc("month", "read_at").alias("read_at_month"))
    .agg(F.count("*").alias("est_count"), F.sum("kwh").alias("est_total_kwh"))
)

ok_monthly = (
    df_meter_readings
    .filter(F.col("quality") == "OK")
    .groupBy(F.date_trunc("month", "read_at").alias("read_at_month"))
    .agg(
        F.count("*").alias("ok_count"),
        F.round(F.avg("kwh"), 2).alias("ok_avg_kwh"),
    )
)

result = (
    est_monthly.alias("a")
    .join(ok_monthly.alias("b"), F.col("a.read_at_month") == F.col("b.read_at_month"), "full")
    .select(
        F.coalesce(F.col("a.read_at_month"), F.col("b.read_at_month")).alias("read_at_month"),
        F.coalesce(F.col("a.est_count"), F.lit(0)).alias("est_count"),
        F.col("a.est_total_kwh"),
        F.coalesce(F.col("b.ok_count"), F.lit(0)).alias("ok_count"),
        F.col("b.ok_avg_kwh"),
        (F.coalesce(F.col("a.est_count"), F.lit(0)) - F.coalesce(F.col("b.ok_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("read_at_month")
)
