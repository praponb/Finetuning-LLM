from pyspark.sql import functions as F

phone_monthly = (
    df_plays
    .filter(F.col("device") == "phone")
    .groupBy(F.date_trunc("month", "played_at").alias("played_at_month"))
    .agg(
        F.count("*").alias("phone_count"),
        F.sum("ms_played").alias("phone_total_ms_played"),
    )
)

car_monthly = (
    df_plays
    .filter(F.col("device") == "car")
    .groupBy(F.date_trunc("month", "played_at").alias("played_at_month"))
    .agg(
        F.count("*").alias("car_count"),
        F.sum("ms_played").alias("car_total_ms_played"),
    )
)

result = (
    phone_monthly.alias("a")
    .join(car_monthly.alias("b"), F.col("a.played_at_month") == F.col("b.played_at_month"), "full")
    .select(
        F.coalesce(F.col("a.played_at_month"), F.col("b.played_at_month")).alias("played_at_month"),
        F.coalesce(F.col("a.phone_count"), F.lit(0)).alias("phone_count"),
        F.col("a.phone_total_ms_played"),
        F.coalesce(F.col("b.car_count"), F.lit(0)).alias("car_count"),
        F.col("b.car_total_ms_played"),
        (F.coalesce(F.col("a.phone_count"), F.lit(0)) - F.coalesce(F.col("b.car_count"), F.lit(0))).alias("phone_count_minus_car_count"),
    )
    .orderBy("played_at_month")
)
