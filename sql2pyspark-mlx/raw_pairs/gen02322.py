from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "attr_key").orderBy(F.col("is_flagged").desc_nulls_first(), "id", "attr_key")
w2 = Window.partitionBy("level").orderBy(F.col("rn").desc(), "id", "attr_key")
w_hist = Window.partitionBy("attr_key", "service").orderBy(F.col("is_flagged").desc())
w4 = Window.partitionBy(F.date_trunc("WEEK", "log_ts"), "level").orderBy("is_flagged", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("service").orderBy("rn", "id", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

parsed = (
    lines
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        "attr_key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("is_flagged"),
    )
)

deduped = (
    parsed
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

ranked = (
    deduped
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
)

metrics = (
    ranked
    .select(
        "id",
        "status_code",
        "attr_key",
        "status_code_high",
        "rn",
        "rn_2",
        F.cume_dist().over(w_hist).alias("is_flagged_cume_dist"),
        F.first("attr_key", ignorenulls=True).over(w4).alias("first_known_attr_key"),
        F.lead("status_code_high", 1, 0).over(w5).alias("next_status_code_high"),
    )
)

filtered = (
    metrics
    .filter(~F.col("attr_key").eqNullSafe(F.lit("version")))
    .withColumn("status_code_high_plus_next_status_code_high", (F.col("status_code_high") + F.col("next_status_code_high")))
    .withColumn("attr_key_clean", F.coalesce("attr_key", F.lit("N/A")))
)

result = (
    filtered
    .groupBy("attr_key")
    .agg(
        F.min("status_code_high_plus_next_status_code_high").alias("min_status_code_high_plus_next_status_code_high"),
        F.count_if(F.col("attr_key") == "version").alias("version_events"),
        F.max(F.when(F.col("attr_key") == "version", F.col("is_flagged_cume_dist"))).alias("version_max_is_flagged_cume_dist"),
    )
    .orderBy(F.col("version_max_is_flagged_cume_dist").desc_nulls_first())
)
