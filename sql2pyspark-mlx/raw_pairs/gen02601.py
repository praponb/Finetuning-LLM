from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("level").orderBy(F.col("status_code_high").desc_nulls_first())
w3 = Window.orderBy(F.col("status_code_cume_dist").asc_nulls_first())
w_grp = Window.partitionBy("level").orderBy(F.col("status_code_cume_dist").desc())
w5 = Window.orderBy(F.col("status_code_high").desc_nulls_first())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

unpacked = (
    lines
    .select(
        "id",
        "level",
        "status_code",
        "attr_value",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

with_windows = (
    unpacked
    .filter(F.col("status_code") > 269)
    .withColumn("status_code_cume_dist", F.cume_dist().over(w))
    .withColumn("status_code_high_percent_rank", F.percent_rank().over(w2))
)

metrics = (
    with_windows
    .withColumn("status_code_cume_dist_cume_dist", F.cume_dist().over(w3))
    .withColumn("status_code_cume_dist_percent_rank", F.percent_rank().over(w_grp))
    .withColumn("status_code_high_dense_rank", F.dense_rank().over(w5))
)

base = (
    metrics
    .filter(F.col("status_code") == 403)
    .select(
        "level",
        "status_code",
        "attr_value",
        "status_code_cume_dist",
        "status_code_cume_dist_cume_dist",
        "status_code_high_dense_rank",
        (F.col("status_code_high_percent_rank") + F.col("status_code_cume_dist")).alias("status_code_high_percent_rank_plus_status_code_cume_dist"),
    )
)

kept_keys = (
    base
    .select("level")
    .exceptAll(
        df_app_logs
        .filter(F.col("service").isin(["search", "checkout", "auth"]))
        .select("level")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("level")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.bit_or("status_code").alias("status_code_bit_or"),
        F.countDistinct("level").alias("distinct_levels"),
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
    )
    .orderBy("distinct_levels")
)
