from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key", "service").orderBy("status_code", "id", "attr_key")
w2 = Window.partitionBy("attr_key").orderBy(F.col("log_ts").desc(), "id", "attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

ranked = (
    flattened
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

base = (
    ranked
    .filter((F.col("log_ts") > F.lit("2025-06-01 07:00:00").cast("timestamp")) & F.col("attr_key").isNotNull())
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        "attr_key",
        "value",
        F.concat("service", F.lit(" - "), "attr_key").alias("service_attr_key"),
        F.coalesce("attr_key", F.lit("NONE")).alias("attr_key_clean"),
    )
)

deduped = (
    base
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

pivoted = (
    deduped
    .groupBy("attr_key", "level")
    .pivot("service", ["auth", "checkout"])
    .agg(F.max("status_code"))
)

result = (
    pivoted
    .select(
        "attr_key",
        "level",
        "checkout",
        F.when(F.col("level").rlike("^(ERROR|DEBUG)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("level_zone"),
    )
    .orderBy("attr_key")
)
