from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("service")
w3 = Window.partitionBy("service").orderBy(F.col("status_code").desc())
w4 = Window.orderBy(F.col("status_code_share_minus_status_code_share_2").asc_nulls_last())
w5 = Window.orderBy(F.col("service_count_status_code").desc_nulls_last())
w6 = Window.partitionBy("k")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

enriched = (
    flattened
    .filter(F.col("k") != "trace")
    .select(
        "k",
        "v",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.count("status_code").over(w2).alias("service_count_status_code"),
        F.percent_rank().over(w3).alias("status_code_percent_rank"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share_2"),
    )
)

filtered = (
    enriched
    .filter(F.col("k").isNotNull() & (~F.col("k").eqNullSafe(F.lit("trace"))))
    .withColumn("status_code_share_minus_status_code_share_2", (F.col("status_code_share") - F.col("status_code_share_2")))
)

cleaned = (
    filtered
    .filter(F.col("k").isNotNull())
    .select(
        "*",
        F.when(F.col("status_code_share") >= 0.84, "High").when(F.col("status_code_share") >= 0.14, "Medium").otherwise("Low").alias("status_code_share_level"),
        F.coalesce("k", F.lit("DIRECT")).alias("resolved_k"),
    )
)

scored = (
    cleaned
    .select(
        "k",
        "v",
        "service_count_status_code",
        "status_code_percent_rank",
        "status_code_share_minus_status_code_share_2",
        F.dense_rank().over(w4).alias("status_code_share_minus_status_code_share_2_dense_rank"),
        F.rank().over(w5).alias("service_count_status_code_rank"),
        F.avg("status_code_share_2").over(w6).alias("k_avg_status_code_share_2"),
    )
)

result = (
    scored
    .select(
        "service_count_status_code",
        "status_code_percent_rank",
        "status_code_share_minus_status_code_share_2_dense_rank",
        "service_count_status_code_rank",
        "k_avg_status_code_share_2",
    )
    .orderBy("service_count_status_code_rank")
)
