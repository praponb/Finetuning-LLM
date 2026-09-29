from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("is_trace").asc_nulls_first())
w2 = Window.partitionBy("attr_key").orderBy(F.col("is_trace_2").desc_nulls_last())

flattened = (
    df_app_logs
    .select("id", "service", "host", F.explode_outer("context").alias("attr_key", "v"))
)

filtered = (
    flattened
    .filter(F.col("service").isin(["billing", "search", "checkout"]))
    .select("id", "attr_key", "v", F.initcap(F.trim("host")).alias("host_title"))
)

valid = (
    filtered
    .filter(~F.col("attr_key").isin(["version", "region"]))
    .select(
        "*",
        F.trim("attr_key").alias("clean_attr_key"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("attr_key_clean"),
    )
)

valid_app_logs = (
    valid
    .filter(F.col("attr_key").isNotNull() & (F.col("attr_key") == "trace"))
    .select(
        "*",
        F.when(F.col("attr_key") == "trace", 1).otherwise(0).alias("is_trace"),
        F.when(F.col("attr_key") == "trace", 1).otherwise(0).alias("is_trace_2"),
    )
)

scored = (
    valid_app_logs
    .filter(F.col("is_trace").between(57, 58))
    .select(
        "*",
        F.rank().over(w).alias("is_trace_rank"),
        F.percent_rank().over(w2).alias("is_trace_2_percent_rank"),
    )
)

report = (
    scored
    .select(
        "id",
        "attr_key",
        "v",
        "host_title",
        "clean_attr_key",
        "is_trace_rank",
        "is_trace_2_percent_rank",
    )
    .orderBy("attr_key")
)

result = (
    report
    .select(
        "id",
        "attr_key",
        "v",
        "host_title",
        "is_trace_rank",
        "is_trace_2_percent_rank",
    )
    .orderBy(F.col("is_trace_2_percent_rank").desc_nulls_first(), F.col("id").desc())
)
