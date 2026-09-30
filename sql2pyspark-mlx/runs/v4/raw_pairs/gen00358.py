from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .filter(F.col("service") == "search")
    .select(
        "id",
        "service",
        "level",
        F.col("context")["region"].alias("region"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("is_flagged"),
        F.col("context").getItem("trace").alias("trace"),
        F.size("context").alias("num_context"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

result = (
    flattened_attrs
    .rollup("service")
    .agg(
        F.min("num_context").alias("min_num_context"),
        F.max("is_flagged").alias("max_is_flagged"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.avg(F.when(F.col("service") == "checkout", F.col("status_code_high"))).alias("checkout_avg_status_code_high"),
        F.sum(F.when(F.col("level") == "INFO", F.col("num_context"))).alias("info_total_num_context"),
        F.grouping("service").alias("grouping_level"),
    )
    .orderBy(F.col("max_is_flagged").desc_nulls_last())
)
