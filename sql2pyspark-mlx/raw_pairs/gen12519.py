from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("key").orderBy("latency_ms", "id", "key")
w2 = Window.partitionBy("service").orderBy(F.col("has_fault").asc_nulls_first(), "id", "key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

unpacked = (
    exploded
    .select(
        "id",
        "service",
        "latency_ms",
        "key",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
    )
)

scored = (
    unpacked
    .filter(F.col("service").isin(["checkout", "billing", "auth"]))
    .select(
        "*",
        F.lead("status_code_high").over(w_grp).alias("next_status_code_high"),
        F.lag("has_fault").over(w_grp).alias("prev_has_fault"),
        F.lead("has_fault").over(w_grp).alias("next_has_fault"),
        F.row_number().over(w_grp).alias("row_seq"),
    )
)

ranked = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .groupBy("service", "key")
    .agg(
        F.size(F.collect_set("service")).alias("num_services"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.count("*").alias("num_rows"),
    )
    .orderBy("key")
)
