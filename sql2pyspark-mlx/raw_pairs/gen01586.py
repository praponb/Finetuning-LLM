from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("status_code").desc(), "id", "attr_key")
w2 = Window.partitionBy(F.to_date("log_ts"), "attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("attr_key")
w4 = Window.partitionBy("level", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

latest = (
    lines
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

remaining_keys = (
    latest
    .select("level")
    .intersect(
        df_app_logs
        .filter(F.col("service").isin(["billing", "search", "auth"]))
        .select("level")
    )
)

verified = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "log_ts",
        "attr_key",
        "v",
        "dedup_rank",
        F.avg("status_code").over(w2).alias("status_code_avg_1h"),
        F.round((F.col("status_code") / F.sum("status_code").over(w3)), 4).alias("status_code_share"),
        F.max("status_code").over(w4).alias("level_max_status_code"),
    )
)

result = (
    enriched
    .withColumn("status_code_avg_1h_tier", F.when(F.col("status_code_avg_1h") >= 384, "PREMIUM").otherwise("STANDARD"))
    .rollup("status_code_avg_1h_tier")
    .agg(
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.count_if(F.col("attr_key") == "region").alias("region_events"),
        F.countDistinct("attr_key").alias("distinct_attr_keys"),
        F.grouping("status_code_avg_1h_tier").alias("grouping_id"),
    )
    .filter(F.col("distinct_attr_keys") > 1)
    .orderBy(F.col("status_code_avg_1h_tier").asc_nulls_first())
)
