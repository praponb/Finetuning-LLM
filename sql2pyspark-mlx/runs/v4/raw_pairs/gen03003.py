from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.partitionBy("service").orderBy(F.col("has_fault").desc())
w3 = Window.partitionBy("level", "service")
w4 = Window.partitionBy("level")
w5 = Window.partitionBy("level").orderBy(F.col("has_fault").asc_nulls_first())
w6 = Window.partitionBy("service", "host")
w7 = Window.partitionBy("host", "level").orderBy(F.col("level_min_service_max_has_fault").desc())
w8 = Window.partitionBy("host").orderBy(F.col("status_code_high").desc_nulls_last())
w_hist = Window.partitionBy("host").orderBy(F.col("status_code_high").desc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

unpacked = (
    exploded
    .select(
        "service",
        "host",
        "level",
        "attr_value",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

enriched = (
    unpacked
    .withColumn("service_max_has_fault", F.max("has_fault").over(w))
    .withColumn("has_fault_rank", F.rank().over(w2))
)

scored = (
    enriched
    .withColumn("level_min_service_max_has_fault", F.min("service_max_has_fault").over(w3))
    .withColumn("service_max_has_fault_2", F.max("has_fault").over(w))
    .withColumn("level_min_has_fault_rank", F.min("has_fault_rank").over(w4))
    .withColumn("has_fault_rank_2", F.rank().over(w5))
)

result = (
    scored
    .select(
        "*",
        F.stddev_samp("service_max_has_fault_2").over(w6).alias("service_stddev_samp_service_max_has_fault_2"),
        F.cume_dist().over(w7).alias("level_min_service_max_has_fault_cume_dist"),
        F.round((F.col("has_fault_rank_2") / F.sum("has_fault_rank_2").over(w4)), 4).alias("has_fault_rank_2_share"),
        F.rank().over(w8).alias("status_code_high_rank"),
        F.cume_dist().over(w_hist).alias("status_code_high_cume_dist"),
    )
)
