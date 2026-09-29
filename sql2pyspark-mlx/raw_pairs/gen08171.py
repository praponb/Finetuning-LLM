from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("host").orderBy("has_fault", "id", "k").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("k", "host").orderBy("has_fault", "id", "k").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("k").orderBy(F.col("status_code_avg_6").asc_nulls_last(), "id", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

parsed = (
    exploded
    .filter(F.col("status_code") < 410)
    .select(
        "id",
        "host",
        "status_code",
        "k",
        "attr_value",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

metrics = (
    parsed
    .filter(F.col("has_fault") == 0)
    .select(
        "id",
        "k",
        "attr_value",
        "status_code_high",
        F.avg("has_fault").over(w_ordered).alias("last7_avg_has_fault"),
        F.count("has_fault").over(w_ordered).alias("last7_count_has_fault"),
        F.min("has_fault").over(w_ordered).alias("last7_min_has_fault"),
        F.sum("has_fault").over(w_ordered).alias("last7_sum_has_fault"),
        F.avg("status_code").over(w2).alias("status_code_avg_6"),
    )
)

deduped = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

cleaned = (
    deduped
    .filter(F.col("last7_min_has_fault").between(0.24, 0.62))
    .select(
        "id",
        "k",
        "attr_value",
        "last7_min_has_fault",
        "last7_sum_has_fault",
        "status_code_avg_6",
        F.greatest("last7_avg_has_fault", "last7_min_has_fault").alias("greatest_last7_avg_has_fault_last7_min_has_fault"),
    )
)

grouped = (
    cleaned
    .withColumn("last7_min_has_fault_tier", F.when(F.col("last7_min_has_fault") >= 0.77, "PREMIUM").otherwise("STANDARD"))
    .withColumn("greatest_last7_avg_has_fault_last7_min_has_fault_tier", F.when(F.col("greatest_last7_avg_has_fault_last7_min_has_fault") >= 66, "PREMIUM").otherwise("STANDARD"))
    .withColumn("status_code_avg_6_tier", F.when(F.col("status_code_avg_6") >= 473, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["last7_min_has_fault_tier", "greatest_last7_avg_has_fault_last7_min_has_fault_tier", "status_code_avg_6_tier"], ["last7_min_has_fault_tier", "greatest_last7_avg_has_fault_last7_min_has_fault_tier"]],
        "last7_min_has_fault_tier",
        "greatest_last7_avg_has_fault_last7_min_has_fault_tier",
        "status_code_avg_6_tier",
    )
    .agg(
        F.percentile_approx("status_code_avg_6", [0.25, 0.5, 0.75]).alias("status_code_avg_6_quartiles"),
        F.count(F.when(F.col("k") == "version", 1)).alias("version_count"),
        F.sum(F.when(F.col("k") == "version", F.col("greatest_last7_avg_has_fault_last7_min_has_fault")).otherwise(0)).alias("version_greatest_last7_avg_has_fault_last7_min_has_fault"),
        F.sum(F.when(F.col("k") == "trace", F.col("last7_min_has_fault"))).alias("trace_total_last7_min_has_fault"),
        F.grouping("last7_min_has_fault_tier").alias("grouping_level"),
    )
    .filter(F.col("trace_total_last7_min_has_fault") > 0)
)

result = (
    grouped
    .select(
        "last7_min_has_fault_tier",
        "greatest_last7_avg_has_fault_last7_min_has_fault_tier",
        "status_code_avg_6_tier",
        "version_count",
        "version_greatest_last7_avg_has_fault_last7_min_has_fault",
        "trace_total_last7_min_has_fault",
    )
    .orderBy(F.col("status_code_avg_6_tier").asc_nulls_first())
)
