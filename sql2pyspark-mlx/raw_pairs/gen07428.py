from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("status_code", "id")
w2 = Window.partitionBy("service")
w3 = Window.orderBy("status_code", "id").rowsBetween(-5, -1)

unpacked = (
    df_app_logs
    .filter(F.col("service").isin(["checkout", "search"]))
    .select(
        "id",
        "service",
        "status_code",
        "payload_json",
        F.col("context")["region"].alias("region"),
        F.size("context").alias("num_context"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("unknown")).alias("trace_or_default"),
    )
)

result = (
    unpacked
    .filter(~F.col("service").isin(["checkout", "search", "auth"]))
    .select(
        "id",
        "status_code",
        "trace_or_default",
        F.lag("num_context", 2).over(w).alias("prev_num_context"),
        F.sum("num_context").over(w2).alias("service_sum_num_context"),
        F.avg("num_context").over(w3).alias("num_context_avg_5_prior"),
        F.lead("num_context", 1).over(w).alias("next_num_context"),
    )
)
