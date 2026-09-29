from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("num_context").desc(), "id")
w2 = Window.partitionBy("level").orderBy("num_context", "id").rowsBetween(-6, -1)

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "context",
        F.col("context")["region"].alias("region"),
        F.size("context").alias("num_context"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.ntile(4).over(w).alias("num_context_bucket"),
        F.avg("num_context").over(w2).alias("num_context_avg_6_prior"),
    )
)
