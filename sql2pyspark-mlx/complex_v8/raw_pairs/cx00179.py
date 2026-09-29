from pyspark.sql import functions as F

est_monthly = (
    df_meter_readings
    .filter(F.col("quality") == "EST")
    .groupBy(F.date_trunc("month", "read_at").alias("read_at_month"))
    .agg(F.count("*").alias("est_count"), F.sum("kwh").alias("est_total_kwh"))
)

bad_monthly = (
    df_meter_readings
    .filter(F.col("quality") == "BAD")
    .groupBy(F.date_trunc("month", "read_at").alias("read_at_month"))
    .agg(
        F.count("*").alias("bad_count"),
        F.round(F.avg("kwh"), 2).alias("bad_avg_kwh"),
    )
)

result = (
    est_monthly.alias("a")
    .join(bad_monthly.alias("b"), F.col("a.read_at_month") == F.col("b.read_at_month"), "full")
    .select(
        F.coalesce(F.col("a.read_at_month"), F.col("b.read_at_month")).alias("read_at_month"),
        F.coalesce(F.col("a.est_count"), F.lit(0)).alias("est_count"),
        F.coalesce(F.col("a.est_total_kwh"), F.lit(0)).alias("est_total_kwh"),
        F.coalesce(F.col("b.bad_count"), F.lit(0)).alias("bad_count"),
        F.col("b.bad_avg_kwh"),
        (F.coalesce(F.col("a.est_count"), F.lit(0)) - F.coalesce(F.col("b.bad_count"), F.lit(0))).alias("est_count_minus_bad_count"),
    )
    .orderBy("read_at_month")
)
