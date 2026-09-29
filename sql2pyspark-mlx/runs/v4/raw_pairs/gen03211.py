from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("status_code_high").desc(), "id", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

parsed = (
    exploded
    .select(
        "id",
        "service",
        "status_code",
        "k",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("is_flagged"),
    )
)

ranked = (
    parsed
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

summary = (
    ranked
    .rollup(
        F.when(F.col("status_code") >= 464, "PREMIUM").otherwise("STANDARD").alias("status_code_tier"),
        "k",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.bit_or("status_code").alias("status_code_bit_or"),
        F.percentile_approx("status_code", 0.5).alias("status_code_p50"),
    )
)

result = summary.select("status_code_tier", "k", "status_code_bit_or", "status_code_p50")
