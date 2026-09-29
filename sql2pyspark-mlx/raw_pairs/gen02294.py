from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("attr_key").orderBy("status_code", "id", "attr_key")
w_part = Window.partitionBy("level").orderBy("latency_ms", "id", "attr_key")
w4 = Window.partitionBy("level")

exploded = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

scored = (
    exploded
    .withColumn("last_known_latency_ms", F.last("latency_ms", ignorenulls=True).over(w))
    .withColumn("prev_status_code", F.lag("status_code", 1).over(w2))
)

metrics = (
    scored
    .filter(F.col("latency_ms").between(996, 1937))
    .select(
        "id",
        "status_code",
        "attr_key",
        "prev_status_code",
        F.lead("latency_ms").over(w_part).alias("next_latency_ms"),
        F.lag("latency_ms").over(w_part).alias("prev_latency_ms"),
        F.lead("prev_status_code").over(w_part).alias("next_prev_status_code"),
        F.row_number().over(w_part).alias("event_seq"),
        F.round((F.col("prev_status_code") / F.sum("prev_status_code").over(w4)), 4).alias("prev_status_code_share"),
    )
)

base = (
    metrics
    .filter(~F.col("attr_key").eqNullSafe(F.lit("region")))
    .select(
        "*",
        F.col("prev_status_code").cast("double").alias("prev_status_code_num"),
        F.floor("status_code").alias("status_code_whole"),
        F.coalesce("attr_key", F.lit("N/A")).alias("attr_key_filled"),
    )
)

stats = (
    base
    .groupBy(
        F.when(F.col("status_code") >= 422, "LARGE").otherwise("SMALL").alias("status_code_tier"),
        F.when(F.col("next_prev_status_code") >= 439, "LARGE").otherwise("SMALL").alias("next_prev_status_code_tier"),
    )
    .agg(
        F.sum("status_code_whole").alias("total_status_code_whole"),
        F.count_if(F.col("attr_key") == "version").alias("version_events"),
        F.count(F.when(F.col("attr_key") == "region", 1)).alias("region_count"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
    )
)

result = (
    stats
    .select(
        "status_code_tier",
        "next_prev_status_code_tier",
        "total_status_code_whole",
        "version_events",
        "region_count",
    )
    .orderBy("total_status_code_whole")
)
