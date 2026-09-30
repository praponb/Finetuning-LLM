from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.partitionBy("host")
w3 = Window.partitionBy("host").orderBy("status_code", "id")
w4 = Window.partitionBy("host", "service").orderBy(F.col("status_code").desc(), "id")
w5 = Window.partitionBy("host").orderBy(F.col("status_code").desc(), "id")

flattened_attrs = (
    df_app_logs
    .filter(F.col("status_code") == 421)
    .select(
        "id",
        "service",
        "host",
        "status_code",
        "context",
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("direct")).alias("trace_or_default"),
        F.col("context").getItem("region").alias("region"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "status_code",
        "trace_or_default",
        "region",
        F.sum("status_code").over(w).alias("service_sum_status_code"),
        F.min("status_code").over(w2).alias("host_min_status_code"),
        F.ntile(5).over(w3).alias("status_code_bucket"),
        F.ntile(3).over(w4).alias("status_code_bucket_2"),
        F.ntile(10).over(w5).alias("status_code_bucket_3"),
    )
)
