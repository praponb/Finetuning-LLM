from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("level", "key")
w2 = Window.partitionBy("level", "key").orderBy("is_flagged", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

unpacked = (
    lines
    .select(
        "id",
        "level",
        "status_code",
        "key",
        "attr_value",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

base = (
    unpacked
    .filter(F.col("key") == "trace")
    .withColumn("level_lower", F.lower("level"))
)

enriched = (
    base
    .select(
        "id",
        "status_code",
        "key",
        "is_flagged",
        "level_lower",
        F.sum("is_flagged").over(w_hist).alias("level_sum_is_flagged"),
        F.sum("is_flagged").over(w2).alias("running_is_flagged"),
        F.avg("is_flagged").over(w_hist).alias("level_avg_is_flagged"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("level_avg_is_flagged") >= 1, "LARGE").otherwise("SMALL").alias("level_avg_is_flagged_tier"),
    )
    .agg(
        F.round(F.avg("level_avg_is_flagged"), 2).alias("avg_level_avg_is_flagged"),
        F.size(F.collect_set("key")).alias("num_keys"),
    )
    .filter(F.col("avg_level_avg_is_flagged") > 0)
    .orderBy(F.col("level_avg_is_flagged_tier").desc())
)
