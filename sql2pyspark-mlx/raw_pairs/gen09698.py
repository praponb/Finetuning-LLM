from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("is_error").desc())
w2 = Window.orderBy("is_error")
w3 = Window.partitionBy("host").orderBy(F.col("is_error").asc_nulls_last())
w4 = Window.partitionBy("attr_key")
w5 = Window.partitionBy("host").orderBy(F.col("is_error").desc_nulls_first())
w6 = Window.orderBy(F.col("attr_key_max_is_error").desc_nulls_last())
w7 = Window.orderBy(F.col("is_error_cume_dist").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

valid = (
    exploded
    .filter(F.col("attr_key").isin(["version", "region", "trace"]))
    .select(
        "*",
        F.substring("service", 1, 3).alias("service_prefix"),
        F.coalesce("attr_key", F.lit("N/A")).alias("attr_key_clean"),
    )
)

matched_keys = (
    valid
    .select("id", "host")
    .subtract(
        df_app_logs
        .filter(F.col("host") == "h4")
        .select("id", "host")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.host") == F.col("k.host")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("attr_key").isNotNull())
    .select(
        "service",
        "host",
        "level",
        "attr_key",
        "attr_value",
        "service_prefix",
        "attr_key_clean",
        F.when(F.col("level") == "ERROR", 1).otherwise(0).alias("is_error"),
        F.concat_ws("-", "service", "attr_key").alias("service_attr_key_key"),
        F.coalesce("attr_key", F.lit("NONE")).alias("attr_key_filled"),
    )
)

scored = (
    base
    .select(
        "service_prefix",
        "attr_key_clean",
        "attr_key_filled",
        F.cume_dist().over(w).alias("is_error_cume_dist"),
        F.rank().over(w2).alias("is_error_rank"),
        F.dense_rank().over(w3).alias("is_error_dense_rank"),
        F.max("is_error").over(w4).alias("attr_key_max_is_error"),
        F.rank().over(w5).alias("is_error_rank_2"),
    )
)

enriched = (
    scored
    .filter(F.col("is_error_dense_rank") >= 3)
    .select(
        "*",
        F.dense_rank().over(w6).alias("attr_key_max_is_error_dense_rank"),
        F.rank().over(w7).alias("is_error_cume_dist_rank"),
    )
)

base_app_logs = (
    enriched
    .filter(F.col("is_error_cume_dist") > 0.21)
    .select(
        "service_prefix",
        "attr_key_clean",
        "is_error_cume_dist",
        "is_error_rank",
        "is_error_dense_rank",
        "is_error_rank_2",
        "attr_key_max_is_error_dense_rank",
        "is_error_cume_dist_rank",
        F.round((F.col("is_error_dense_rank") / F.col("attr_key_max_is_error_dense_rank")), 2).alias("is_error_dense_rank_per_attr_key_max_is_error_dense_rank"),
    )
)

result = (
    base_app_logs
    .groupBy(
        F.when(F.col("is_error_rank_2") >= 7, "PREMIUM").otherwise("STANDARD").alias("is_error_rank_2_tier"),
        F.when(F.col("is_error_cume_dist") >= 0.68, "LARGE").otherwise("SMALL").alias("is_error_cume_dist_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("row_count"),
        F.percentile_approx("is_error_dense_rank", [0.25, 0.5, 0.75]).alias("is_error_dense_rank_quartiles"),
    )
)
