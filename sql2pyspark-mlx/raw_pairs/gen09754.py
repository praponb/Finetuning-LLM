from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "status_code",
        F.col("context").getItem("region").alias("region"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("direct")).alias("trace_or_default"),
    )
)
