from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("status_code_high_per_has_fault").desc())
w2 = Window.partitionBy("service").orderBy(F.col("status_code_high").desc())
w3 = Window.partitionBy("service").orderBy("status_code_high_per_has_fault")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

flattened_attrs = (
    exploded
    .select(
        "id",
        "service",
        "level",
        "status_code",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
    )
)

base = (
    flattened_attrs
    .filter(F.col("status_code_high") >= 1)
    .select(
        "service",
        "status_code_high",
        "has_fault",
        F.upper("service").alias("service_upper"),
    )
)

valid = (
    base
    .filter(F.col("status_code_high") <= 2)
    .select("*", F.ceil("has_fault").alias("has_fault_whole"))
)

base_app_logs = (
    valid
    .filter(F.col("status_code_high") <= 2)
    .withColumn("status_code_high_per_has_fault", (F.col("status_code_high") / F.nullif("has_fault", F.lit(0))))
    .withColumn("status_code_high_minus_has_fault", (F.col("status_code_high") - F.col("has_fault")))
)

metrics = (
    base_app_logs
    .select(
        "*",
        F.percent_rank().over(w).alias("status_code_high_per_has_fault_percent_rank"),
        F.dense_rank().over(w2).alias("status_code_high_dense_rank"),
        F.cume_dist().over(w3).alias("status_code_high_per_has_fault_cume_dist"),
    )
)

result = (
    metrics
    .select(
        "service",
        "has_fault",
        "service_upper",
        "has_fault_whole",
        "status_code_high_per_has_fault",
        "status_code_high_minus_has_fault",
        "status_code_high_per_has_fault_percent_rank",
        "status_code_high_dense_rank",
    )
    .orderBy(F.col("status_code_high_minus_has_fault").desc_nulls_first())
)
