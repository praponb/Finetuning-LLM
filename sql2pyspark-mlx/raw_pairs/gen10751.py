from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select("id", "host", "level", F.explode_outer("context").alias("k", "value"))
)

result = (
    lines
    .groupBy("level")
    .agg(
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.count(F.when(F.col("host") == "h1", 1)).alias("h1_count"),
        F.size(F.collect_set("k")).alias("num_ks"),
        F.count(F.when(F.col("level") == "INFO", 1)).alias("info_count"),
        F.count(F.when(F.col("k") == "trace", 1)).alias("trace_rows"),
    )
    .orderBy(F.col("trace_rows").desc_nulls_last())
)
