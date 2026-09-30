from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_code_high_plus_is_flagged")
w2 = Window.orderBy(F.col("status_code_high_whole_per_status_code_high").desc_nulls_first())
w3 = Window.orderBy(F.col("status_code_high").desc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

parsed = (
    exploded
    .filter(F.col("status_code") > 303)
    .select(
        "id",
        "status_code",
        "value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("is_flagged"),
    )
)

base = (
    parsed
    .filter(F.col("is_flagged") > 0)
    .select(
        "id",
        "status_code",
        "value",
        "status_code_high",
        "is_flagged",
        F.floor("status_code_high").alias("status_code_high_whole"),
        (F.col("status_code_high") * F.col("is_flagged")).alias("status_code_high_times_is_flagged"),
    )
)

cleaned = (
    base
    .filter(F.col("status_code_high") > 1)
    .select(
        "*",
        (F.col("status_code_high") + F.col("is_flagged")).alias("status_code_high_plus_is_flagged"),
    )
)

filtered = (
    cleaned
    .filter(F.col("status_code_high") == 1)
    .select(
        "id",
        "status_code",
        "status_code_high",
        "is_flagged",
        "status_code_high_plus_is_flagged",
        F.when(F.col("status_code") >= 421, "High").when(F.col("status_code") >= 258, "Medium").otherwise("Low").alias("status_code_tier"),
        (F.col("status_code_high_whole") / F.nullif("status_code_high", F.lit(0))).alias("status_code_high_whole_per_status_code_high"),
    )
)

enriched = (
    filtered
    .withColumn("status_code_high_plus_is_flagged_dense_rank", F.dense_rank().over(w))
    .withColumn("status_code_high_whole_per_status_code_high_cume_dist", F.cume_dist().over(w2))
    .withColumn("status_code_high_cume_dist", F.cume_dist().over(w3))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("status_code_high_plus_is_flagged_dense_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("status_code_high_plus_is_flagged_dense_rank_tier"),
        F.when(F.col("is_flagged") >= 1, "PREMIUM").otherwise("STANDARD").alias("is_flagged_tier"),
    )
    .agg(
        F.avg("status_code_high_plus_is_flagged").alias("avg_status_code_high_plus_is_flagged"),
        F.max("status_code_high").alias("max_status_code_high"),
        F.round(F.avg("is_flagged"), 2).alias("avg_is_flagged"),
        F.percentile_approx("status_code_high_plus_is_flagged_dense_rank", 0.5).alias("status_code_high_plus_is_flagged_dense_rank_p50"),
        F.percentile_approx("is_flagged", [0.25, 0.5, 0.75]).alias("is_flagged_quartiles"),
        F.percentile_approx("is_flagged", 0.9).alias("is_flagged_p90"),
    )
    .filter(F.col("avg_is_flagged") > 0)
)
