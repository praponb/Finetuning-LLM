from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

parsed = (
    exploded
    .filter(F.col("status_code").between(275, 314))
    .select(
        "status_code",
        "attr_value",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

base = (
    parsed
    .filter(F.col("status_code").between(348, 359))
    .withColumn("flag_bit1_tier", F.when(F.col("flag_bit1") >= 1, "High").when(F.col("flag_bit1") >= 0, "Medium").otherwise("Low"))
    .withColumn("adjusted_status_code_high", F.round((F.col("status_code_high") * 1.1), 2))
)

base_app_logs = (
    base
    .filter(F.col("status_code") <= 413)
    .withColumn("status_code_whole", F.floor("status_code"))
)

result = (
    base_app_logs
    .groupBy(
        F.when(F.col("status_code_whole") >= 70, "HIGH").otherwise("LOW").alias("status_code_whole_tier"),
    )
    .agg(
        F.avg("status_code_whole").alias("avg_status_code_whole"),
        F.count("*").alias("cnt"),
        F.round(F.avg("status_code_high"), 2).alias("avg_status_code_high"),
        F.percentile_approx("status_code_high", 0.5).alias("status_code_high_p50"),
        F.percentile_approx("status_code_high", 0.9).alias("status_code_high_p90"),
    )
)
