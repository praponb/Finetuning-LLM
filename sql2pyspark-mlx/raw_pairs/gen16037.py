from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "service").orderBy("status_code")
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("level", "attr_key").orderBy("status_code")
w4 = Window.partitionBy("level")
w5 = Window.partitionBy("level", "attr_key")
w6 = Window.partitionBy(F.year("log_ts"), "level").orderBy("level_max_status_code", "id", "attr_key")
w7 = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key")
w8 = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

enriched = (
    exploded
    .withColumn("status_code_rank", F.rank().over(w))
    .withColumn("status_code_avg_30m", F.avg("status_code").over(w2))
    .withColumn("status_code_cume_dist", F.cume_dist().over(w3))
    .withColumn("level_max_status_code", F.max("status_code").over(w4))
)

with_windows = (
    enriched
    .select(
        "*",
        F.avg("status_code").over(w5).alias("level_avg_status_code"),
        F.ntile(10).over(w6).alias("level_max_status_code_bucket"),
    )
)

flagged = (
    with_windows
    .withColumn("new_group_flag", F.when(F.lag("log_ts").over(w7).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w7) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w8))

wide = (
    sessionized
    .groupBy("service")
    .pivot("attr_key", ["region", "version"])
    .agg(
        F.max("level_max_status_code_bucket").alias("value_sum"),
        F.count(F.lit(1)).alias("n"),
    )
)

result = (
    wide
    .select(
        "service",
        "region_value_sum",
        "region_n",
        "version_n",
        F.when(F.col("service").rlike("^(billing|checkout)$"), "PRIORITY").otherwise("CORE").alias("service_zone"),
    )
    .orderBy(F.col("service_zone").asc_nulls_last())
)
