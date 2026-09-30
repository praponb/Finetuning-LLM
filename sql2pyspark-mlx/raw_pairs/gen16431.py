from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code_high").desc_nulls_last())
w2 = Window.partitionBy("attr_key").orderBy(F.col("status_code").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

parsed = (
    lines
    .select(
        "host",
        "log_ts",
        "status_code",
        "attr_key",
        "v",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

scored = (
    parsed
    .select(
        "status_code",
        "attr_key",
        F.dense_rank().over(w).alias("status_code_high_dense_rank"),
        F.dense_rank().over(w2).alias("status_code_dense_rank"),
    )
)

valid = (
    scored
    .filter(F.col("status_code_dense_rank") == 8)
    .withColumn("status_code_high_dense_rank_per_status_code_dense_rank", F.round((F.col("status_code_high_dense_rank") / F.col("status_code_dense_rank")), 2))
    .withColumn("status_code_high_dense_rank_filled", F.coalesce("status_code_high_dense_rank", F.lit(0)))
)

result = (
    valid
    .groupBy("attr_key")
    .agg(
        F.bit_or("status_code").alias("status_code_bit_or"),
        F.count("*").alias("event_count"),
        F.max("status_code_dense_rank").alias("max_status_code_dense_rank"),
        F.round(F.avg("status_code_high_dense_rank_filled"), 2).alias("avg_status_code_high_dense_rank_filled"),
        F.round(F.sum("status_code_high_dense_rank_filled") / F.nullif(F.count(F.when(F.col("attr_key") == "version", 1)), F.lit(0)), 2).alias("status_code_high_dense_rank_filled_per_version"),
        F.bit_and("status_code").alias("status_code_bit_and"),
    )
    .filter(F.col("status_code_bit_and") < 1000)
    .orderBy("status_code_bit_and")
)
