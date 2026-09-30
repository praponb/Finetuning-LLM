from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("adjusted_status_code", "id", "attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

flattened_attrs = (
    flattened
    .select(
        "id",
        "level",
        "status_code",
        "attr_key",
        "value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
    )
)

base = (
    flattened_attrs
    .filter(F.col("attr_key").isNotNull())
    .select(
        "*",
        F.substring("level", 1, 2).alias("level_prefix"),
        F.round((F.col("status_code") * 1.07), 2).alias("adjusted_status_code"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("attr_key_clean"),
    )
)

ranked = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "level",
        "status_code",
        "attr_key",
        "has_fault",
        "adjusted_status_code",
        "attr_key_clean",
    )
    .orderBy(F.col("adjusted_status_code").desc())
)
