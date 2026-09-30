from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("has_fault").desc())
w2 = Window.partitionBy("attr_key").orderBy(F.col("status_code").desc(), "id", "attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

parsed = (
    exploded
    .filter(F.col("service") == "auth")
    .select(
        "id",
        "service",
        "host",
        "status_code",
        "attr_key",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

latest = (
    parsed
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

latest_app_logs = (
    latest
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

valid = (
    latest_app_logs
    .filter(F.col("status_code_high") > 1)
    .withColumn("is_billing", F.when(F.col("service") == "billing", 1).otherwise(0))
    .withColumn("resolved_attr_key", F.coalesce("attr_key", F.lit("OTHER")))
)

stats = (
    valid
    .groupBy(
        F.when(F.col("status_code") >= 443, "PREMIUM").otherwise("STANDARD").alias("status_code_tier"),
    )
    .agg(
        F.bit_and("status_code").alias("status_code_bit_and"),
        F.countDistinct("attr_key").alias("distinct_attr_keys"),
        F.count_if(F.col("service") == "auth").alias("auth_events"),
        F.min("status_code_high").alias("min_status_code_high"),
        F.round(F.sum("status_code") / F.nullif(F.count(F.when(F.col("attr_key") == "region", 1)), F.lit(0)), 2).alias("status_code_per_region"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
    )
)

result = (
    stats
    .select(
        "status_code_tier",
        "auth_events",
        "min_status_code_high",
        "status_code_per_region",
        "attr_key_list",
    )
    .orderBy(F.col("auth_events").asc_nulls_last())
)
