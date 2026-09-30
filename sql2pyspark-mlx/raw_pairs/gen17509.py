from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("log_ts")).orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("key")
w_running = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_recent = Window.partitionBy(F.to_date("log_ts")).orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w6 = Window.partitionBy("key", "service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

valid = (
    flattened
    .filter(F.col("key").isNotNull())
    .withColumn("log_ts_weekofyear", F.weekofyear("log_ts"))
    .withColumn("is_search", F.when(F.col("service") == "search", 1).otherwise(0))
)

enriched = (
    valid
    .select(
        "*",
        F.max("is_search").over(w).alias("is_search_max_1h"),
        F.count("is_search").over(w2).alias("is_search_count_1h"),
        F.count("is_search").over(w3).alias("key_count_is_search"),
    )
)

filtered = (
    enriched
    .filter(F.col("key").isNotNull() & (F.col("status_code") == 438))
    .select("*", F.when(F.col("service") == "auth", 1).otherwise(0).alias("is_auth"))
)

with_windows = (
    filtered
    .withColumn("is_search_count_1h_max_2h", F.max("is_search_count_1h").over(w_running))
    .withColumn("is_search_count_1h_share", F.round((F.col("is_search_count_1h") / F.sum("is_search_count_1h").over(w3)), 4))
    .withColumn("key_max_is_search", F.max("is_search").over(w3))
    .withColumn("log_ts_weekofyear_sum_15m", F.sum("log_ts_weekofyear").over(w_recent))
    .withColumn("is_search_count_1h_sum_2h", F.sum("is_search_count_1h").over(w6))
)

result = (
    with_windows
    .withColumn("hour_window", F.date_trunc("HOUR", "log_ts"))
    .groupingSets([["service", "hour_window"], ["service"]], "service", "hour_window")
    .agg(
        F.size(F.collect_set("service")).alias("num_services"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("key").alias("distinct_keys"),
        F.max(F.when(F.col("key") == "trace", F.col("is_search_count_1h_share"))).alias("trace_max_is_search_count_1h_share"),
    )
    .filter(F.col("trace_max_is_search_count_1h_share") < 1000000)
    .orderBy("distinct_ids")
)
