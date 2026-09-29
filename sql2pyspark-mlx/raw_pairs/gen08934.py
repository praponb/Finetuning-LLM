from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "attr_key").orderBy("status_code_high", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level").orderBy(F.col("running_max_is_flagged").desc_nulls_first(), "id", "attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

flattened_attrs = (
    exploded
    .filter(F.col("attr_key").isin(["region", "trace", "version"]))
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        "attr_key",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("is_flagged"),
    )
)

metrics = (
    flattened_attrs
    .filter(F.col("status_code") > 436)
    .select(
        "id",
        "level",
        "attr_key",
        "attr_value",
        "is_flagged",
        F.count("is_flagged").over(w).alias("running_count_is_flagged"),
        F.min("is_flagged").over(w).alias("running_min_is_flagged"),
        F.sum("is_flagged").over(w).alias("running_sum_is_flagged"),
        F.max("is_flagged").over(w).alias("running_max_is_flagged"),
    )
)

ranked = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .groupBy("level")
    .agg(
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_count"),
        F.min("running_sum_is_flagged").alias("min_running_sum_is_flagged"),
    )
    .orderBy(F.col("min_running_sum_is_flagged").desc())
)
