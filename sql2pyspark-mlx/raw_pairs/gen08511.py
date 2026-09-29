from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy(F.month("log_ts"), "key").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("key").orderBy("log_ts_day", "id", "key")
w3 = Window.partitionBy("key").orderBy(F.col("version_rows").desc())

lines = (
    df_app_logs
    .select("id", "host", "log_ts", F.explode("context").alias("key", "attr_value"))
)

enriched = (
    lines
    .select(
        "id",
        "host",
        "log_ts",
        "key",
        F.last("key", ignorenulls=True).over(w_ordered).alias("last_known_key"),
    )
)

valid = (
    enriched
    .filter(F.col("host") == "h1")
    .select(
        "id",
        "key",
        "last_known_key",
        F.col("log_ts").cast("date").alias("log_ts_day"),
    )
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .groupBy("key", F.date_trunc("QUARTER", "log_ts_day").alias("quarter_start"))
    .agg(
        F.count(F.when(F.col("key") == "version", 1)).alias("version_rows"),
        F.count(F.when(F.col("key") == "region", 1)).alias("region_count"),
        F.count(F.when(F.col("key") == "region", 1)).alias("region_count_2"),
    )
    .withColumn("version_rows_dense_rank", F.dense_rank().over(w3))
)
