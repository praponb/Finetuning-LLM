from pyspark.sql import functions as F

valid = (
    df_sessions
    .filter((F.col("browser").eqNullSafe(F.lit("Safari"))) & (F.col("started_at") > F.to_date(F.lit("2023-03-29"))))
    .select(
        "id",
        "device",
        "country",
        F.length("device").alias("device_length"),
        F.coalesce("country", F.lit("N/A")).alias("resolved_country"),
    )
)

result = (
    valid
    .cube(
        F.when(F.col("device_length") >= 78, "PREMIUM").otherwise("STANDARD").alias("device_length_tier"),
    )
    .agg(
        F.max("device_length").alias("max_device_length"),
        F.round(F.avg("device_length"), 2).alias("avg_device_length"),
        F.max(F.when(F.col("country") == "JP", F.col("device_length"))).alias("jp_max_device_length"),
        F.percentile_approx("device_length", [0.25, 0.5, 0.75]).alias("device_length_quartiles"),
    )
    .orderBy("jp_max_device_length")
)
