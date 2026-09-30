from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_code")
w2 = Window.partitionBy("attr_key")
w3 = Window.orderBy(F.col("status_code_cume_dist").asc_nulls_first())
w4 = Window.partitionBy("attr_key").orderBy("status_code_cume_dist")
w5 = Window.orderBy(F.col("attr_key_length").asc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

with_windows = (
    lines
    .select(
        "*",
        F.cume_dist().over(w).alias("status_code_cume_dist"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share"),
    )
)

valid = (
    with_windows
    .filter(F.col("status_code_cume_dist").between(0.3, 0.75))
    .select(
        "id",
        "attr_key",
        "attr_value",
        "status_code_cume_dist",
        F.length("attr_key").alias("attr_key_length"),
    )
)

metrics = (
    valid
    .filter(F.col("attr_key").isin(["version", "trace"]))
    .select(
        "id",
        "attr_value",
        F.sum("status_code_cume_dist").over(w2).alias("attr_key_sum_status_code_cume_dist"),
        F.cume_dist().over(w3).alias("status_code_cume_dist_cume_dist"),
        F.rank().over(w4).alias("status_code_cume_dist_rank"),
        F.dense_rank().over(w5).alias("attr_key_length_dense_rank"),
    )
)

base = (
    metrics
    .filter(F.col("status_code_cume_dist_rank") < 5)
    .select(
        "*",
        F.round((F.col("status_code_cume_dist_rank") * 0.9), 2).alias("adjusted_status_code_cume_dist_rank"),
        F.round("attr_key_sum_status_code_cume_dist", 0).alias("attr_key_sum_status_code_cume_dist_rounded"),
    )
)

result = (
    base
    .select(
        "attr_value",
        "attr_key_sum_status_code_cume_dist",
        "status_code_cume_dist_rank",
        "adjusted_status_code_cume_dist_rank",
        "attr_key_sum_status_code_cume_dist_rounded",
    )
    .orderBy(F.col("status_code_cume_dist_rank").desc_nulls_first())
)
