from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("log_ts").desc(), "id", "attr_key")
w2 = Window.partitionBy("level").orderBy("log_ts", "id", "attr_key")
w3 = Window.partitionBy("level").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

deduped = (
    flattened
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

gaps = (
    deduped
    .withColumn("new_group_flag", F.when(F.lag("log_ts").over(w2).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w2) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("new_group_flag").over(w3))

filtered = (
    islands
    .filter((F.col("log_ts") >= F.lit("2025-06-01 14:00:00").cast("timestamp")) & F.col("attr_key").isNotNull())
    .withColumn("log_ts_day", F.dayofmonth("log_ts"))
)

result = (
    filtered
    .groupBy("level", "host")
    .pivot("attr_key", ["trace", "version"])
    .agg(F.max("new_group_flag").alias("amount"))
)
