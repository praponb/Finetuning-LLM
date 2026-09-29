from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("status_code_high").desc_nulls_first())
w2 = Window.orderBy(F.col("status_code_high").desc_nulls_first())
w3 = Window.partitionBy("service")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

flattened_attrs = (
    lines
    .select(
        "id",
        "service",
        "value",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

remaining_keys = (
    flattened_attrs
    .select("service")
    .subtract(
        df_app_logs
        .filter(F.col("level") != "INFO")
        .select("service")
    )
)

kept_rows = (
    flattened_attrs
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .filter(F.col("service") == "auth")
    .select(
        "*",
        F.cume_dist().over(w).alias("status_code_high_cume_dist"),
        F.rank().over(w2).alias("status_code_high_rank"),
        F.max("is_flagged").over(w3).alias("service_max_is_flagged"),
    )
)

result = (
    scored
    .groupBy("service")
    .agg(
        F.max("is_flagged").alias("max_is_flagged"),
        F.count("*").alias("event_count"),
        F.percentile_approx("status_code_high_cume_dist", 0.9).alias("status_code_high_cume_dist_p90"),
    )
    .filter(F.col("status_code_high_cume_dist_p90") > 0)
)
