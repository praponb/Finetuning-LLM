from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("is_flagged")
w2 = Window.orderBy(F.col("status_code_high").asc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

parsed = (
    flattened
    .filter(~F.col("key").isin(["region", "version"]))
    .select(
        "id",
        "level",
        "key",
        "v",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

base = (
    parsed
    .filter(F.col("key").isNotNull() & (F.col("level") == "DEBUG"))
    .withColumn("is_flagged_tier", F.when(F.col("is_flagged") >= 1, "High").when(F.col("is_flagged") >= 0, "Medium").otherwise("Low"))
    .withColumn("key_title", F.initcap(F.trim("key")))
    .withColumn("key_filled", F.coalesce("key", F.lit("NONE")))
)

metrics = (
    base
    .select(
        "*",
        F.percent_rank().over(w).alias("is_flagged_percent_rank"),
        F.cume_dist().over(w2).alias("status_code_high_cume_dist"),
    )
)

result = (
    metrics
    .select(
        "key",
        "v",
        "is_flagged",
        "status_code_high",
        "is_flagged_tier",
        "key_title",
        "key_filled",
        "is_flagged_percent_rank",
        "status_code_high_cume_dist",
        F.when(F.col("key").rlike("^(version|trace)$"), "GROUP_B").otherwise("EXTENDED").alias("key_zone"),
    )
    .orderBy(F.col("status_code_high").asc_nulls_first(), F.col("is_flagged").desc())
)
