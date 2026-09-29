from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.partitionBy(F.year("log_ts"), "service")
w4 = Window.partitionBy(F.year("log_ts")).orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w5 = Window.partitionBy("service").orderBy(F.col("status_code_high").desc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

unpacked = (
    exploded
    .filter(F.col("log_ts") < "2025-09-19")
    .select(
        "id",
        "service",
        "log_ts",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

cleaned = (
    unpacked
    .filter(F.col("status_code_high") < 2)
    .withColumn("years_since_log_ts", (F.datediff(F.current_date(), "log_ts") / 365))
)

kept_keys = (
    cleaned
    .select("id", "service")
    .exceptAll(
        df_app_logs
        .filter(F.col("latency_ms") <= 2252)
        .select("id", "service")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .withColumn("service_count_status_code_high", F.count("status_code_high").over(w))
    .withColumn("years_since_log_ts_avg_2h", F.avg("years_since_log_ts").over(w2))
    .withColumn("log_ts_year_max_status_code_high", F.max("status_code_high").over(w3))
    .withColumn("years_since_log_ts_avg_1h", F.avg("years_since_log_ts").over(w4))
    .withColumn("status_code_high_cume_dist", F.cume_dist().over(w5))
)

grouped = (
    enriched
    .groupBy(
        F.when(F.col("years_since_log_ts") >= 74, "PREMIUM").otherwise("STANDARD").alias("years_since_log_ts_tier"),
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
    )
    .agg(
        F.size(F.collect_set("service")).alias("num_services"),
        F.size(F.collect_set("service")).alias("num_services_2"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
    )
    .filter(F.col("num_services_2") >= 1)
)

result = (
    grouped
    .select("years_since_log_ts_tier", "activity_day", "num_services")
    .orderBy("years_since_log_ts_tier")
)
