from pyspark.sql import functions as F

prepared = (
    df_warehouses
    .filter((F.upper(F.trim(F.col("wh_code"))).like("K%")) & (F.col("city") == "Bangkok"))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.coalesce(F.col("capacity"), F.lit(0)).alias("capacity_filled"),
        F.coalesce(F.col("city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
    )
)

rollup_base = (
    prepared
    .filter(F.col("capacity") <= 11506)
    .groupBy("city_or_unknown", "city")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("capacity_filled"), 2).alias("avg_capacity_filled"),
        F.avg("capacity").alias("avg_capacity_check"),
    )
    .filter(F.col("avg_capacity_check") > 4912)
    .drop("avg_capacity_check")
)

derived = (
    rollup_base
    .withColumn("least_cnt_avg_capacity_filled", F.least(F.col("cnt"), F.col("avg_capacity_filled")))
)

screened = (
    derived
    .crossJoin(derived.agg(F.avg("cnt").alias("overall_avg_cnt")))
    .filter(F.col("cnt") > F.col("overall_avg_cnt"))
    .drop("overall_avg_cnt")
)

result = (
    screened
    .select(
        "city_or_unknown",
        "city",
        "cnt",
        "avg_capacity_filled",
        "least_cnt_avg_capacity_filled",
        F.length(F.trim(F.col("city_or_unknown"))).alias("city_or_unknown_len"),
    )
)
