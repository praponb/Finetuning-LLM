from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_code")
w2 = Window.partitionBy("level", "key").orderBy("status_code_per_status_code_dense_rank")
w3 = Window.orderBy(F.col("status_code_per_status_code_dense_rank").asc_nulls_last())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

scored = flattened.select("*", F.dense_rank().over(w).alias("status_code_dense_rank"))

base = (
    scored
    .filter(F.col("status_code") != 389)
    .select("*", F.initcap(F.trim("level")).alias("level_title"))
)

filtered = (
    base
    .filter(F.col("key").isNotNull() & F.col("service").isin(["auth", "billing", "checkout"]))
    .select(
        "id",
        "level",
        "key",
        F.round((F.col("status_code") / F.col("status_code_dense_rank")), 2).alias("status_code_per_status_code_dense_rank"),
        F.coalesce("key", F.lit("DIRECT")).alias("key_clean"),
    )
)

metrics = (
    filtered
    .select(
        "level",
        "key",
        "status_code_per_status_code_dense_rank",
        F.rank().over(w2).alias("status_code_per_status_code_dense_rank_rank"),
    )
)

enriched = (
    metrics
    .filter(F.col("status_code_per_status_code_dense_rank") > 76)
    .withColumn("status_code_per_status_code_dense_rank_dense_rank", F.dense_rank().over(w3))
)

result = (
    enriched
    .cube(
        F.when(F.col("status_code_per_status_code_dense_rank") >= 81, "LARGE").otherwise("SMALL").alias("status_code_per_status_code_dense_rank_tier"),
    )
    .agg(
        F.count_if(F.col("level") == "INFO").alias("info_events"),
        F.countDistinct("key").alias("distinct_keys"),
    )
    .orderBy(F.col("info_events").desc_nulls_first())
)
