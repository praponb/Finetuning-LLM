from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("status_code", "id", "attr_key").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("service").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("service").orderBy(F.col("status_code").desc())
w4 = Window.partitionBy("service", "level").orderBy(F.col("status_code_avg_3").desc_nulls_first(), "id", "attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

enriched = (
    flattened
    .filter(F.col("status_code") == 422)
    .withColumn("status_code_avg_3", F.avg("status_code").over(w))
    .withColumn("first_known_attr_key", F.first("attr_key", ignorenulls=True).over(w2))
    .withColumn("status_code_rank", F.rank().over(w3))
)

latest = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "service",
        "level",
        "attr_key",
        "v",
        "status_code_avg_3",
        "first_known_attr_key",
        "status_code_rank",
    )
)

matched_keys = (
    latest
    .select("service", "level")
    .subtract(
        df_app_logs
        .filter(F.col("latency_ms").between(949, 1538))
        .select("service", "level")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.service") == F.col("k.service")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("level").isin(["INFO", "DEBUG", "ERROR"]))
    .withColumn("status_code_avg_3_filled", F.coalesce("status_code_avg_3", F.lit(0)))
    .withColumn("attr_key_clean", F.coalesce("attr_key", F.lit("UNKNOWN")))
)

result = (
    base
    .select(
        "id",
        "level",
        "attr_key",
        "v",
        "status_code_avg_3",
        "first_known_attr_key",
        "status_code_rank",
        F.when(F.col("attr_key").rlike("^(version|region)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("attr_key_zone"),
    )
    .orderBy(F.col("status_code_avg_3").desc())
)
