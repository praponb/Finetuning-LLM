from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("flag_bit2_rounded")
w2 = Window.partitionBy("level", "host").orderBy("status_code_high")
w3 = Window.partitionBy("host")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

flattened_attrs = (
    exploded
    .select(
        "service",
        "host",
        "level",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
    )
)

filtered = (
    flattened_attrs
    .filter(F.col("service").isin(["auth", "checkout", "billing"]))
    .select("*", F.round("flag_bit2", 2).alias("flag_bit2_rounded"))
)

kept_keys = (
    filtered
    .select("host", "service")
    .subtract(
        df_app_logs
        .filter(F.col("host") == "h2")
        .select("host", "service")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .withColumn("flag_bit2_rounded_rank", F.rank().over(w))
    .withColumn("status_code_high_dense_rank", F.dense_rank().over(w2))
)

metrics = (
    enriched
    .select(
        "service",
        "host",
        "level",
        "v",
        "status_code_high",
        "flag_bit2",
        "flag_bit2_rounded",
        "status_code_high_dense_rank",
        F.avg("flag_bit2_rounded").over(w3).alias("host_avg_flag_bit2_rounded"),
    )
)

result = (
    metrics
    .rollup(
        F.when(F.col("flag_bit2_rounded") >= 66, "PREMIUM").otherwise("STANDARD").alias("flag_bit2_rounded_tier"),
        F.when(F.col("host_avg_flag_bit2_rounded") >= 83, "PREMIUM").otherwise("STANDARD").alias("host_avg_flag_bit2_rounded_tier"),
        "level",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.min("status_code_high").alias("min_status_code_high"),
        F.percentile_approx("status_code_high", [0.25, 0.5, 0.75]).alias("status_code_high_quartiles"),
        F.count(F.when(F.col("host") == "h2", 1)).alias("h2_count"),
    )
    .orderBy(F.col("level").desc_nulls_last())
)
