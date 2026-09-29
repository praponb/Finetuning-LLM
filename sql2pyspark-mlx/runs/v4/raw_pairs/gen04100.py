from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "k")
w2 = Window.partitionBy("service").orderBy("status_code", "id", "k").rowsBetween(-2, -1)
w3 = Window.orderBy("status_code", "id", "k")
w4 = Window.partitionBy("k").orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("k")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

filtered = (
    flattened
    .filter(F.col("k").isNotNull() & F.col("k").isin(["trace", "region"]))
    .select(
        "*",
        F.substring("k", 1, 3).alias("k_prefix"),
        F.upper("k").alias("k_upper"),
    )
)

scored = (
    filtered
    .withColumn("service_min_latency_ms", F.min("latency_ms").over(w))
    .withColumn("status_code_avg_2_prior", F.avg("status_code").over(w2))
    .withColumn("prev_status_code", F.lag("status_code", 1, 0).over(w3))
    .withColumn("cumulative_status_code", F.sum("status_code").over(w4))
    .withColumn("k_count_latency_ms", F.count("latency_ms").over(w5))
)

result = (
    scored
    .select(
        "id",
        "service",
        "status_code",
        "k",
        "value",
        "k_upper",
        "service_min_latency_ms",
        "status_code_avg_2_prior",
        "cumulative_status_code",
        F.when(F.col("service").rlike("^(search|auth)$"), "CORE").when(F.col("service").rlike("^(billing|checkout)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("service_zone"),
    )
)
