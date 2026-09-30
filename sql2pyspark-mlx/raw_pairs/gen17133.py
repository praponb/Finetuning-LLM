from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy(F.col("status_code").desc())
w2 = Window.orderBy(F.col("status_code").desc())
w3 = Window.partitionBy("key").orderBy(F.col("status_code_cume_dist").desc_nulls_last())
w4 = Window.partitionBy("key").orderBy(F.col("status_code_cume_dist").asc_nulls_first())
w5 = Window.partitionBy("key")
w_part = Window.orderBy(F.col("status_code_cume_dist_cume_dist").desc_nulls_first())
w7 = Window.orderBy(F.col("status_code_cume_dist_cume_dist").asc_nulls_last())
w8 = Window.orderBy(F.col("key_max_status_code_cume_dist").desc())
w_grp = Window.orderBy(F.col("status_code_cume_dist_dense_rank").desc_nulls_last())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

with_windows = (
    flattened
    .withColumn("status_code_cume_dist", F.cume_dist().over(w))
    .withColumn("status_code_rank", F.rank().over(w2))
)

valid = (
    with_windows
    .filter(F.col("key").isNotNull() & (F.col("service") == "billing"))
    .select(
        "id",
        "status_code",
        "key",
        "attr_value",
        "status_code_cume_dist",
        "status_code_rank",
        (F.col("status_code_cume_dist") - F.col("status_code_rank")).alias("status_code_cume_dist_minus_status_code_rank"),
        F.coalesce("key", F.lit("DIRECT")).alias("resolved_key"),
    )
)

valid_app_logs = (
    valid
    .filter(~F.col("key").eqNullSafe(F.lit("trace")))
    .select(
        "status_code",
        "key",
        "attr_value",
        "status_code_cume_dist",
        "resolved_key",
        F.round((F.col("status_code") * 1.1), 2).alias("adjusted_status_code"),
    )
)

filtered = (
    valid_app_logs
    .filter(F.col("key").eqNullSafe(F.lit("region")))
    .select(
        "*",
        F.col("status_code").cast("string").alias("status_code_str"),
        F.when(F.col("key").isin(["version", "region"]), "Tier 1").otherwise("Other").alias("key_group"),
    )
)

with_windows_app_logs = (
    filtered
    .select(
        "attr_value",
        "status_code_cume_dist",
        "resolved_key",
        "status_code_str",
        F.dense_rank().over(w3).alias("status_code_cume_dist_dense_rank"),
        F.cume_dist().over(w4).alias("status_code_cume_dist_cume_dist"),
        F.max("status_code_cume_dist").over(w5).alias("key_max_status_code_cume_dist"),
    )
)

scored = (
    with_windows_app_logs
    .filter(F.col("status_code_cume_dist_dense_rank").between(4, 7))
    .withColumn("status_code_cume_dist_cume_dist_dense_rank", F.dense_rank().over(w_part))
    .withColumn("status_code_cume_dist_cume_dist_cume_dist", F.cume_dist().over(w7))
    .withColumn("key_max_status_code_cume_dist_rank", F.rank().over(w8))
    .withColumn("status_code_cume_dist_dense_rank_percent_rank", F.percent_rank().over(w_grp))
)

result = (
    scored
    .groupBy(
        F.when(F.col("key_max_status_code_cume_dist_rank") >= 8, "PREMIUM").otherwise("STANDARD").alias("key_max_status_code_cume_dist_rank_tier"),
        F.when(F.col("key_max_status_code_cume_dist") >= 1, "LARGE").otherwise("SMALL").alias("key_max_status_code_cume_dist_tier"),
        F.when(F.col("status_code_cume_dist_dense_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("status_code_cume_dist_dense_rank_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("status_code_cume_dist_cume_dist_dense_rank", 0.5).alias("status_code_cume_dist_cume_dist_dense_rank_p50"),
        F.min("status_code_cume_dist_cume_dist").alias("min_status_code_cume_dist_cume_dist"),
    )
)
