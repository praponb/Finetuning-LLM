from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k")
w2 = Window.partitionBy("service").orderBy(F.col("k_sum_status_code_str").desc_nulls_first())
w3 = Window.partitionBy("service", "k").orderBy(F.col("k_stddev_samp_status_code_str").desc())
w4 = Window.partitionBy("level")
w5 = Window.orderBy(F.col("k_sum_status_code_str").desc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

filtered = (
    exploded
    .filter((F.col("k").eqNullSafe(F.lit("version"))) & (F.col("level") == "ERROR"))
    .select("*", F.col("status_code").cast("double").alias("status_code_str"))
)

with_windows = (
    filtered
    .filter(F.col("level").isin(["DEBUG", "INFO", "WARN"]))
    .withColumn("k_sum_status_code_str", F.sum("status_code_str").over(w))
)

base = (
    with_windows
    .filter(F.col("level").isin(["ERROR", "DEBUG", "INFO"]))
    .withColumn("status_code_per_k_sum_status_code_str", (F.col("status_code") / F.nullif("k_sum_status_code_str", F.lit(0))))
)

with_windows_app_logs = (
    base
    .withColumn("k_stddev_samp_status_code_str", F.stddev_samp("status_code_str").over(w))
    .withColumn("k_sum_status_code_str_percent_rank", F.percent_rank().over(w2))
)

metrics = (
    with_windows_app_logs
    .select(
        "*",
        F.dense_rank().over(w3).alias("k_stddev_samp_status_code_str_dense_rank"),
        F.sum("status_code_per_k_sum_status_code_str").over(w4).alias("level_sum_status_code_per_k_sum_status_code_str"),
        F.cume_dist().over(w5).alias("k_sum_status_code_str_cume_dist"),
    )
)

matched_keys = (
    metrics
    .select("id")
    .subtract(
        df_app_logs
        .filter(F.col("service") == "auth")
        .select("id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("status_code_str_tier", F.when(F.col("status_code_str") >= 77, "HIGH").otherwise("LOW"))
    .withColumn("status_code_per_k_sum_status_code_str_tier", F.when(F.col("status_code_per_k_sum_status_code_str") >= 85, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["status_code_str_tier", "status_code_per_k_sum_status_code_str_tier"], ["status_code_str_tier"]],
        "status_code_str_tier",
        "status_code_per_k_sum_status_code_str_tier",
    )
    .agg(
        F.bit_or("status_code").alias("status_code_bit_or"),
        F.sum(F.when(F.col("host") == "h1", F.col("k_stddev_samp_status_code_str"))).alias("h1_total_k_stddev_samp_status_code_str"),
        F.count(F.when(F.col("host") == "h3", 1)).alias("h3_count"),
    )
    .filter(F.col("status_code_bit_or") > 1)
    .orderBy(F.col("status_code_str_tier").desc())
)
