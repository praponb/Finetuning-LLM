from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select("id", "host", "log_ts", F.explode_outer("context").alias("k", "attr_value"))
)

valid = (
    exploded
    .filter((F.col("log_ts") > F.lit("2025-06-01 15:00:00").cast("timestamp")) & (~F.col("k").eqNullSafe(F.lit("version"))))
    .withColumn("host_k", F.concat("host", F.lit(" - "), "k"))
    .withColumn("k_clean", F.coalesce("k", F.lit("N/A")))
)

base = (
    valid
    .filter((F.col("log_ts") > F.to_timestamp(F.lit("2025-06-01 14:00:00"))) & (F.col("k") != "version"))
    .withColumn("log_ts_year", F.year("log_ts"))
    .withColumn("k_filled", F.coalesce("k", F.lit("Unknown")))
    .withColumn("k_clean_2", F.coalesce("k", F.lit("N/A")))
)

base_app_logs = (
    base
    .filter(F.col("log_ts") > F.lit("2025-06-01").cast("date"))
    .select(
        "id",
        "log_ts",
        "attr_value",
        "host_k",
        "k_clean",
        "log_ts_year",
        F.concat("host", F.lit(", "), "k").alias("host_k_2"),
        F.initcap(F.trim("k")).alias("k_title"),
    )
)

result = (
    base_app_logs
    .select(
        "id",
        "attr_value",
        "host_k",
        "k_clean",
        "log_ts_year",
        "host_k_2",
        "k_title",
    )
    .orderBy(F.col("id").asc_nulls_first())
)
