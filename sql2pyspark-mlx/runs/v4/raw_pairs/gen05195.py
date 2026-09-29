from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

base = (
    flattened
    .filter((F.col("log_ts") > F.lit("2025-06-01 18:00:00").cast("timestamp")) & (F.col("service") != "checkout"))
    .select("id", "service", F.trim("key").alias("clean_key"))
)

cleaned = (
    base
    .filter(F.col("service") == "auth")
    .select("*", F.initcap(F.trim("service")).alias("service_title"))
)

base_app_logs = (
    cleaned
    .filter(F.col("service").isin(["search", "billing"]))
    .withColumn("service_title_2", F.initcap(F.trim("service")))
)

result = (
    base_app_logs
    .groupBy("service")
    .agg(
        F.count_if(F.col("service") == "search").alias("search_events"),
        F.size(F.collect_set("service")).alias("num_services"),
        F.size(F.collect_set("service")).alias("num_services_2"),
        F.size(F.collect_set("service")).alias("num_services_3"),
        F.count(F.when(F.col("service") == "checkout", 1)).alias("checkout_rows"),
    )
)
