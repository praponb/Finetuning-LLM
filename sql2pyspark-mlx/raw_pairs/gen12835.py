from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("k").orderBy("is_flagged", "id", "k").rowsBetween(-6, -1)
w2 = Window.partitionBy("k").orderBy("status_code_high", "id", "k")
w3 = Window.partitionBy("k").orderBy(F.col("status_code_high").desc_nulls_last())
w4 = Window.partitionBy("k").orderBy(F.col("status_code_high_rank").desc(), "id", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

parsed = (
    exploded
    .filter(F.col("k") == "region")
    .select(
        "id",
        "status_code",
        "k",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("is_flagged"),
    )
)

with_windows = (
    parsed
    .filter(F.col("k").isin(["version", "trace"]))
    .withColumn("status_code_avg_6_prior", F.avg("status_code").over(w_grp))
    .withColumn("status_code_high_bucket", F.ntile(10).over(w2))
    .withColumn("status_code_high_rank", F.rank().over(w3))
)

deduped = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .groupBy("k")
    .agg(
        F.size(F.collect_set("k")).alias("num_ks"),
        F.sum(F.when(F.col("k") == "region", F.col("status_code_high_bucket")).otherwise(0)).alias("region_status_code_high_bucket"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
    )
    .orderBy(F.col("k").desc())
)
