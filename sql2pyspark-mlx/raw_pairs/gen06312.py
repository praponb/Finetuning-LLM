from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy(F.col("is_flagged_minus_status_code").desc_nulls_first())
w_running = Window.partitionBy("host").orderBy(F.col("is_flagged_minus_status_code").desc_nulls_last())
w3 = Window.orderBy(F.col("is_flagged_minus_status_code").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

unpacked = (
    flattened
    .select(
        "id",
        "service",
        "host",
        "status_code",
        "attr_key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
    )
)

base = (
    unpacked
    .filter(F.col("status_code") < 383)
    .withColumn("service_title", F.initcap(F.trim("service")))
    .withColumn("service_attr_key_key", F.concat_ws("-", "service", "attr_key"))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("N/A")))
)

valid = (
    base
    .filter(F.col("attr_key").isNotNull())
    .select(
        "host",
        "status_code",
        "status_code_high",
        "is_flagged",
        "service_title",
        "service_attr_key_key",
        "attr_key_filled",
        (F.col("is_flagged") - F.col("status_code")).alias("is_flagged_minus_status_code"),
    )
)

enriched = (
    valid
    .filter(F.col("is_flagged_minus_status_code").between(48, 56))
    .withColumn("is_flagged_minus_status_code_rank", F.rank().over(w))
    .withColumn("is_flagged_minus_status_code_percent_rank", F.percent_rank().over(w_running))
    .withColumn("is_flagged_minus_status_code_dense_rank", F.dense_rank().over(w3))
)

result = (
    enriched
    .groupBy("host")
    .agg(
        F.percentile_approx("is_flagged_minus_status_code_dense_rank", [0.25, 0.5, 0.75]).alias("is_flagged_minus_status_code_dense_rank_quartiles"),
        F.round(F.sum("status_code_high") / F.nullif(F.count(F.when(F.col("host") == "h1", 1)), F.lit(0)), 2).alias("status_code_high_per_h1"),
        F.sum(F.when(F.col("host") == "h4", F.col("is_flagged_minus_status_code_rank")).otherwise(0)).alias("h4_is_flagged_minus_status_code_rank"),
    )
    .orderBy(F.col("status_code_high_per_h1").desc())
)
