from pyspark.sql import functions as F

remaining_keys = (
    df_sessions
    .select("browser", "device")
    .subtract(
        df_sessions
        .filter(F.col("started_at") >= "2023-09-01")
        .select("browser", "device")
    )
)

reconciled = (
    df_sessions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")) & (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(F.date_trunc("WEEK", "started_at").alias("week_start"))
    .agg(
        F.count(F.when(F.col("browser") == "Chrome", 1)).alias("chrome_count"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.min("started_at").alias("started_at"),
        F.countDistinct("device").alias("distinct_devices"),
        F.size(F.collect_set("device")).alias("num_devices"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .filter(F.col("num_browsers") > 1)
)
