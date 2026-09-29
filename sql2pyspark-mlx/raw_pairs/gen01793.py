from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code_high").asc_nulls_last())
w2 = Window.orderBy("has_fault")
w3 = Window.orderBy(F.col("has_fault").desc_nulls_last())
w4 = Window.orderBy(F.col("status_code_high").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

unpacked = (
    lines
    .select(
        "id",
        "attr_value",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

with_windows = (
    unpacked
    .filter(F.col("status_code_high").isNull())
    .withColumn("status_code_high_percent_rank", F.percent_rank().over(w))
    .withColumn("has_fault_rank", F.rank().over(w2))
    .withColumn("status_code_high_rank", F.rank().over(w))
    .withColumn("has_fault_percent_rank", F.percent_rank().over(w3))
    .withColumn("status_code_high_rank_2", F.rank().over(w4))
)

matched_keys = (
    with_windows
    .select("id")
    .intersect(
        df_app_logs
        .filter(F.col("host") == "h3")
        .select("id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("has_fault") > 1)
    .withColumn("greatest_status_code_high_rank_has_fault", F.greatest("status_code_high_rank", "has_fault"))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("has_fault_rank") >= 8, "HIGH").otherwise("LOW").alias("has_fault_rank_tier"),
        F.when(F.col("status_code_high_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("status_code_high_rank_tier"),
    )
    .agg(
        F.sum("status_code_high_rank").alias("total_status_code_high_rank"),
        F.percentile_approx("has_fault_rank", 0.95).alias("has_fault_rank_p95"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("status_code_high").alias("total_status_code_high"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .orderBy(F.col("total_status_code_high").desc())
)
