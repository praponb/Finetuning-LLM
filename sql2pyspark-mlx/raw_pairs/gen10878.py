from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.partitionBy("service").orderBy(F.col("status_code").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

cleaned = (
    lines
    .filter(F.col("key").isNotNull() & (F.col("key") != "region"))
    .select(
        "*",
        F.upper("key").alias("key_upper"),
        F.when(F.col("status_code") >= 444, "High").when(F.col("status_code") >= 268, "Medium").otherwise("Low").alias("status_code_level"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "service",
        "key",
        "v",
        "key_upper",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share_2"),
        F.dense_rank().over(w2).alias("status_code_dense_rank"),
    )
)

matched_keys = (
    enriched
    .select("id", "service")
    .exceptAll(
        df_app_logs
        .filter(F.col("log_ts") >= "2024-06-13")
        .select("id", "service")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("service")
    .agg(
        F.sum(F.when(F.col("service") == "search", F.col("status_code_dense_rank")).otherwise(0)).alias("search_status_code_dense_rank"),
        F.sum(F.when(F.col("service") == "auth", F.col("status_code_share_2")).otherwise(0)).alias("auth_status_code_share_2"),
    )
    .filter(F.col("auth_status_code_share_2") < 1000000)
    .orderBy(F.col("auth_status_code_share_2").desc_nulls_first())
)
