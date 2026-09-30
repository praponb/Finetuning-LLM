from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("status_code", "id", "attr_key")
w2 = Window.partitionBy("attr_key", "host").orderBy("status_code", "id", "attr_key")
w_recent = Window.partitionBy("attr_key").orderBy("status_code", "id", "attr_key").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("attr_key").orderBy(F.col("last7_avg_status_code").asc_nulls_last(), "id", "attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

valid = (
    exploded
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        "attr_key",
        F.when(F.col("level").isin(["INFO", "WARN"]), "Core").otherwise("Other").alias("level_group"),
        F.concat_ws(",", "attr_key", "level").alias("attr_key_level_key"),
    )
)

ranked = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

ranked_app_logs = (
    ranked
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "status_code",
        "attr_key",
        "level_group",
        "attr_key_level_key",
        "rank_in_group",
    )
)

metrics = (
    ranked_app_logs
    .filter(F.col("rank_in_group") > 1)
    .select(
        "id",
        "attr_key",
        "attr_key_level_key",
        "rank_in_group",
        F.count("status_code").over(w_recent).alias("last7_count_status_code"),
        F.min("status_code").over(w_recent).alias("last7_min_status_code"),
        F.max("status_code").over(w_recent).alias("last7_max_status_code"),
        F.avg("status_code").over(w_recent).alias("last7_avg_status_code"),
    )
)

deduped = (
    metrics
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("last7_count_status_code") >= 14, "PREMIUM").otherwise("STANDARD").alias("last7_count_status_code_tier"),
        "attr_key",
    )
    .agg(
        F.round(F.avg("last7_avg_status_code"), 2).alias("avg_last7_avg_status_code"),
        F.avg(F.when(F.col("attr_key") == "region", F.col("last7_min_status_code"))).alias("region_avg_last7_min_status_code"),
        F.percentile_approx("last7_min_status_code", 0.9).alias("last7_min_status_code_p90"),
        F.max("last7_max_status_code").alias("max_last7_max_status_code"),
    )
)
