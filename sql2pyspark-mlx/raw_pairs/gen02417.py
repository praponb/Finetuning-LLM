from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        F.col("context")["region"].alias("region"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.coalesce(F.col("context")["trace"], F.lit("direct")).alias("trace_or_default"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.col("context")["trace"].alias("trace"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("is_flagged") >= 1, "HIGH").otherwise("LOW").alias("is_flagged_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count("*").alias("row_count_2"),
        F.sum("status_code_high").alias("total_status_code_high"),
    )
)
