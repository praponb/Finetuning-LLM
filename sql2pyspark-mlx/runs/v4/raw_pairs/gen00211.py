from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("status_code_high").asc_nulls_last())
w2 = Window.partitionBy("service").orderBy("status_code_high")
w_part = Window.partitionBy("service").orderBy(F.col("status_code_high").desc_nulls_last())
w_recent = Window.partitionBy("service")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

unpacked = (
    exploded
    .select(
        "id",
        "service",
        "attr_value",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

enriched = (
    unpacked
    .withColumn("status_code_high_dense_rank", F.dense_rank().over(w))
    .withColumn("status_code_high_rank", F.rank().over(w2))
)

scored = (
    enriched
    .withColumn("status_code_high_dense_rank_2", F.dense_rank().over(w_part))
    .withColumn("service_sum_status_code_high", F.sum("status_code_high").over(w_recent))
    .withColumn("status_code_high_percent_rank", F.percent_rank().over(w_part))
    .withColumn("service_sum_status_code_high_2", F.sum("status_code_high").over(w_recent))
    .withColumn("status_code_high_percent_rank_2", F.percent_rank().over(w2))
)

totals = (
    scored
    .groupBy(
        F.when(F.col("status_code_high_percent_rank_2") >= 0.74, "HIGH").otherwise("LOW").alias("status_code_high_percent_rank_2_tier"),
    )
    .agg(
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_rows"),
        F.size(F.collect_set("service")).alias("num_services"),
        F.percentile_approx("has_fault", [0.25, 0.5, 0.75]).alias("has_fault_quartiles"),
        F.percentile_approx("status_code_high_dense_rank", 0.9).alias("status_code_high_dense_rank_p90"),
        F.countDistinct("service").alias("distinct_services"),
        F.sum(F.when(F.col("service") == "search", F.col("status_code_high"))).alias("search_total_status_code_high"),
    )
    .filter(F.col("billing_rows") >= 1)
)

result = (
    totals
    .select(
        "status_code_high_percent_rank_2_tier",
        "num_services",
        "status_code_high_dense_rank_p90",
        "search_total_status_code_high",
    )
    .orderBy(
        F.col("search_total_status_code_high").desc(),
        F.col("status_code_high_dense_rank_p90").desc(),
    )
)
