from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        F.explode_outer("context").alias("key", "v"),
    )
)

result = (
    flattened
    .groupBy("key")
    .agg(
        F.size(F.collect_set("key")).alias("num_keys"),
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_count"),
        F.size(F.collect_set("key")).alias("num_keys_2"),
    )
    .filter(F.col("num_keys_2") >= 1)
)
