from pyspark.sql import functions as F

bad_monthly = (
    df_meter_readings
    .filter(F.col("quality") == "BAD")
    .groupBy(F.date_trunc("month", "read_at").alias("read_at_month"))
    .agg(F.count("*").alias("bad_count"), F.sum("kwh").alias("bad_total_kwh"))
)

ok_monthly = (
    df_meter_readings
    .filter(F.col("quality") == "OK")
    .groupBy(F.date_trunc("month", "read_at").alias("read_at_month"))
    .agg(F.count("*").alias("ok_count"), F.sum("kwh").alias("ok_total_kwh"))
)

result = (
    bad_monthly.alias("a")
    .join(ok_monthly.alias("b"), F.col("a.read_at_month") == F.col("b.read_at_month"), "full")
    .select(
        F.coalesce(F.col("a.read_at_month"), F.col("b.read_at_month")).alias("read_at_month"),
        F.coalesce(F.col("a.bad_count"), F.lit(0)).alias("bad_count"),
        F.col("a.bad_total_kwh"),
        F.coalesce(F.col("b.ok_count"), F.lit(0)).alias("ok_count"),
        F.coalesce(F.col("b.ok_total_kwh"), F.lit(0)).alias("ok_total_kwh"),
        (F.coalesce(F.col("a.bad_count"), F.lit(0)) - F.coalesce(F.col("b.ok_count"), F.lit(0))).alias("bad_count_minus_ok_count"),
    )
    .orderBy("read_at_month")
)
