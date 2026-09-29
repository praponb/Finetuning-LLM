from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("host").isin(["h2", "h4", "h1"]))
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        F.coalesce(F.col("context").getItem("region"), F.lit("none")).alias("region_or_default"),
        F.coalesce(F.col("context")["trace"], F.lit("direct")).alias("trace_or_default"),
    )
)
