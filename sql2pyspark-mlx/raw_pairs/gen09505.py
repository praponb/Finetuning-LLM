from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

unpacked = (
    exploded
    .filter(F.col("status_code") <= 417)
    .select(
        "service",
        "status_code",
        "k",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

base = (
    unpacked
    .filter(F.col("k").isNotNull() & F.col("service").isin(["auth", "search", "checkout"]))
    .select(
        "*",
        F.col("status_code_high").cast("double").alias("status_code_high_num"),
        F.coalesce("k", F.lit("N/A")).alias("resolved_k"),
    )
)

valid = (
    base
    .filter(F.col("is_flagged") < 0)
    .select(
        "service",
        "status_code",
        "is_flagged",
        "status_code_high",
        "status_code_high_num",
        "resolved_k",
        (F.col("status_code_high_num") + F.col("status_code")).alias("status_code_high_num_plus_status_code"),
        F.coalesce("k", F.lit("OTHER")).alias("resolved_k_2"),
    )
)

base_app_logs = (
    valid
    .filter(F.col("status_code_high").isNotNull())
    .select(
        "service",
        "status_code_high",
        "status_code_high_num",
        "resolved_k",
        "status_code_high_num_plus_status_code",
        "resolved_k_2",
        F.when(F.col("service") == "search", 1).otherwise(0).alias("is_search"),
        F.col("status_code_high_num").cast("int").alias("status_code_high_num_int"),
    )
)

result = (
    base_app_logs
    .groupBy(
        "service",
        F.when(F.col("status_code_high") >= 3, "HIGH").otherwise("LOW").alias("status_code_high_tier"),
        F.when(F.col("status_code_high_num") >= 71, "PREMIUM").otherwise("STANDARD").alias("status_code_high_num_tier"),
    )
    .agg(
        F.count(F.when(F.col("service") == "checkout", 1)).alias("checkout_rows"),
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_rows"),
        F.max(F.when(F.col("service") == "checkout", F.col("status_code_high_num"))).alias("checkout_max_status_code_high_num"),
        F.count(F.when(F.col("service") == "search", 1)).alias("search_rows"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
    )
)
