from pyspark.sql import functions as F

ok_monthly = (
    df_meter_readings
    .filter(F.col("quality") == "OK")
    .groupBy(F.date_trunc("month", "read_at").alias("read_at_month"))
    .agg(F.count("*").alias("ok_count"), F.sum("kwh").alias("ok_total_kwh"))
)

est_monthly = (
    df_meter_readings
    .filter(F.col("quality") == "EST")
    .groupBy(F.date_trunc("month", "read_at").alias("read_at_month"))
    .agg(F.count("*").alias("est_count"), F.sum("kwh").alias("est_total_kwh"))
)

result = (
    ok_monthly.alias("a")
    .join(est_monthly.alias("b"), F.col("a.read_at_month") == F.col("b.read_at_month"), "full")
    .select(
        F.coalesce(F.col("a.read_at_month"), F.col("b.read_at_month")).alias("read_at_month"),
        F.coalesce(F.col("a.ok_count"), F.lit(0)).alias("ok_count"),
        F.col("a.ok_total_kwh"),
        F.coalesce(F.col("b.est_count"), F.lit(0)).alias("est_count"),
        F.col("b.est_total_kwh"),
        (F.coalesce(F.col("a.ok_count"), F.lit(0)) - F.coalesce(F.col("b.est_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("read_at_month")
)
