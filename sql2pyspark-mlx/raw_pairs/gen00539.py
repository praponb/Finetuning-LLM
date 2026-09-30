from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k")
w2 = Window.partitionBy("k", "service")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

scored = (
    flattened
    .filter(F.col("status_code") <= 334)
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
    .withColumn("latency_ms_share_2", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4))
)

cleaned = (
    scored
    .filter(F.col("log_ts") >= F.lit("2025-06-01").cast("date"))
    .select(
        "service",
        "status_code",
        "k",
        "attr_value",
        "latency_ms_share",
        F.when(F.col("service").isin(["checkout", "search"]), "Tier 1").otherwise("Other").alias("service_group"),
        F.coalesce("k", F.lit("DIRECT")).alias("k_clean"),
    )
)

kept_keys = (
    cleaned
    .select("service")
    .subtract(
        df_app_logs
        .filter(~F.col("service").isin(["billing", "search", "checkout"]))
        .select("service")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("service") == "checkout")
    .select(
        "status_code",
        "k",
        "attr_value",
        "service_group",
        F.round((F.col("status_code") * 1.15), 2).alias("status_code_adjusted"),
    )
)

valid = (
    base
    .filter(F.col("status_code") == 337)
    .select(
        "*",
        F.lower("k").alias("k_lower"),
        F.coalesce("k", F.lit("NONE")).alias("k_clean"),
    )
)

result = (
    valid
    .select(
        "status_code",
        "k",
        "attr_value",
        "service_group",
        "status_code_adjusted",
        "k_lower",
        "k_clean",
    )
    .orderBy(F.col("status_code").asc_nulls_first())
)
