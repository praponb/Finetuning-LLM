from pyspark.sql import functions as F

flattened = df_app_logs.select("id", F.explode_outer("context").alias("attr_key", "attr_value"))

base = (
    flattened
    .filter(F.col("attr_key").isNotNull() & (F.col("attr_key") == "version"))
    .withColumn("attr_key_upper", F.upper("attr_key"))
)

matched_keys = (
    base
    .select("id")
    .intersectAll(
        df_app_logs
        .filter(F.col("service") == "billing")
        .select("id")
    )
)

verified = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("attr_key").isNotNull() & (F.col("attr_key") != "region"))
    .select("*", F.length("attr_key").alias("attr_key_length"))
)

matched_keys_2 = (
    valid
    .select("id")
    .subtract(
        df_app_logs
        .filter(F.col("level").isin(["WARN", "INFO", "DEBUG"]))
        .select("id")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("attr_key")
    .agg(
        F.sum(F.when(F.col("attr_key") == "trace", F.col("attr_key_length")).otherwise(0)).alias("trace_attr_key_length"),
        F.count(F.when(F.col("attr_key") == "trace", 1)).alias("trace_count"),
    )
    .filter(F.col("trace_count") > 1)
    .orderBy(F.col("trace_count").desc())
)
