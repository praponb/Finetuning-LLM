from pyspark.sql import functions as F

scoped_plays = df_plays.filter((F.col("is_skipped")) & (F.col("ms_played") >= 73335))

rollup_base = (
    scoped_plays
    .groupBy(
        F.date_trunc("month", "played_at").alias("played_at_month"),
        "is_skipped",
    )
    .agg(
        F.percentile_approx("ms_played", 0.5).alias("median_ms_played"),
        F.count(F.when(F.col("device").isin("car", "phone"), 1)).alias("n_device_in_car_phone"),
        F.sum("ms_played").alias("total_ms_played"),
        F.sum(F.when(F.col("device") != "car", 1).otherwise(0)).alias("n_device_car"),
    )
)

without_matches = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("n_device_in_car_phone").alias("overall_avg_n_device_in_car_phone")))
    .filter(F.col("n_device_in_car_phone") > F.col("overall_avg_n_device_in_car_phone"))
    .drop("overall_avg_n_device_in_car_phone")
)

tagged = (
    without_matches
    .filter(F.col("n_device_car") > 3)
    .select(
        "*",
        F.date_trunc("day", F.col("played_at_month")).alias("played_at_month_day"),
        F.to_date(F.col("played_at_month")).alias("played_at_month_date"),
    )
)

result = (
    tagged
    .select(
        "played_at_month",
        "median_ms_played",
        "n_device_in_car_phone",
        "n_device_car",
        "played_at_month_day",
        "played_at_month_date",
        F.trunc(F.col("played_at_month_date"), "MM").alias("played_at_month_date_month_start"),
        F.round(F.col("n_device_car") / F.nullif(F.col("n_device_in_car_phone"), F.lit(0)), 3).alias("n_device_car_per_n_device_in_car_phone"),
    )
    .orderBy(
        F.col("n_device_car_per_n_device_in_car_phone").desc(),
        F.col("n_device_in_car_phone").desc(),
    )
)
