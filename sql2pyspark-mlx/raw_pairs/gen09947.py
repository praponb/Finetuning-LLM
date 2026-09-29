from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("status_code")
w2 = Window.orderBy(F.col("flag_bit2").desc())
w3 = Window.partitionBy("attr_key")
w4 = Window.partitionBy("level")
w5 = Window.partitionBy("attr_key").orderBy(F.col("status_code_percent_rank").desc_nulls_first())
w6 = Window.partitionBy("level").orderBy("flag_bit2_cume_dist")
w7 = Window.partitionBy("attr_key").orderBy(F.col("flag_bit2").asc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

flattened_attrs = (
    lines
    .select(
        "level",
        "status_code",
        "attr_key",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
    )
)

kept_keys = (
    flattened_attrs
    .select("level")
    .subtract(
        df_app_logs
        .filter(F.col("latency_ms") != 2055)
        .select("level")
    )
)

verified = (
    flattened_attrs
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

scored = (
    verified
    .withColumn("status_code_percent_rank", F.percent_rank().over(w))
    .withColumn("flag_bit2_cume_dist", F.cume_dist().over(w2))
)

scored_app_logs = (
    scored
    .select(
        "*",
        F.round((F.col("status_code") / F.sum("status_code").over(w3)), 4).alias("status_code_share"),
        F.max("status_code_percent_rank").over(w4).alias("level_max_status_code_percent_rank"),
        F.cume_dist().over(w5).alias("status_code_percent_rank_cume_dist"),
        F.dense_rank().over(w6).alias("flag_bit2_cume_dist_dense_rank"),
        F.percent_rank().over(w7).alias("flag_bit2_percent_rank"),
    )
)

result = (
    scored_app_logs
    .select(
        "level",
        "status_code",
        "attr_key",
        "v",
        "status_code_high",
        "flag_bit2",
        "flag_bit2_cume_dist",
        "status_code_share",
        "status_code_percent_rank_cume_dist",
    )
    .orderBy(F.col("status_code_percent_rank_cume_dist").desc(), "level")
)
