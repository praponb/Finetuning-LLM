from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("log_ts").desc(), "id", "k")

flattened = (
    df_app_logs
    .select("id", "log_ts", "latency_ms", F.explode("context").alias("k", "v"))
)

ranked = (
    flattened
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "k", "v")
)

filtered = (
    ranked
    .filter(F.col("k") == "trace")
    .withColumn("k_lower", F.lower("k"))
    .withColumn("k_clean", F.coalesce("k", F.lit("UNKNOWN")))
)

base = (
    filtered
    .filter(F.col("k").isNotNull() & (~F.col("k").eqNullSafe(F.lit("trace"))))
    .withColumn("k_prefix", F.substring("k", 1, 4))
    .withColumn("k_upper", F.upper("k"))
    .withColumn("resolved_k", F.coalesce("k", F.lit("NONE")))
)

result = (
    base
    .select("id", "k", "v", "k_clean", "k_prefix", "k_upper", "resolved_k")
    .orderBy("k")
)
