from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

flattened_attrs = (
    flattened
    .filter(F.col("service") == "search")
    .select(
        "service",
        "log_ts",
        "key",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

base = (
    flattened_attrs
    .filter(F.col("is_flagged") < 1)
    .select(
        "service",
        "log_ts",
        "is_flagged",
        F.to_date("log_ts").alias("log_ts_date"),
        F.substring("service", 1, 2).alias("service_prefix"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("key_filled"),
    )
)

result = (
    base
    .groupBy("service")
    .agg(
        F.min("log_ts").alias("first_seen"),
        F.sum(F.when(F.col("service") == "checkout", F.col("is_flagged"))).alias("checkout_total_is_flagged"),
        F.sum(F.when(F.col("service") == "billing", F.col("is_flagged")).otherwise(0)).alias("billing_is_flagged"),
    )
    .filter(F.col("checkout_total_is_flagged") >= 0)
    .orderBy(F.col("checkout_total_is_flagged").asc_nulls_first())
)
