from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("hour_window").orderBy(F.col("version_count").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

result = (
    flattened
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"), "key")
    .agg(
        F.count(F.when(F.col("key") == "version", 1)).alias("version_count"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
    )
    .withColumn("version_count_dense_rank", F.dense_rank().over(w))
)
